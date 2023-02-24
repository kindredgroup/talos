use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use log::{debug, error, info};

use tokio::sync::mpsc;

use crate::{
    core::{DecisionOutboxChannelMessage, MessageVariant, System, SystemService},
    errors::{SystemServiceError, SystemServiceErrorKind},
    model::DecisionMessage,
    ports::{DecisionStore, MessagePublisher},
    SystemMessage,
};

pub struct DecisionOutboxService {
    pub system: System,
    pub decision_outbox_channel_tx: mpsc::Sender<DecisionOutboxChannelMessage>,
    pub decision_outbox_channel_rx: mpsc::Receiver<DecisionOutboxChannelMessage>,
    pub decision_store: Arc<Box<dyn DecisionStore<Decision = DecisionMessage> + Sync + Send>>,
    pub decision_publisher: Arc<Box<dyn MessagePublisher + Sync + Send>>,
}

impl DecisionOutboxService {
    pub fn new(
        decision_outbox_channel_tx: mpsc::Sender<DecisionOutboxChannelMessage>,
        decision_outbox_channel_rx: mpsc::Receiver<DecisionOutboxChannelMessage>,
        decision_store: Arc<Box<dyn DecisionStore<Decision = DecisionMessage> + Sync + Send>>,
        decision_publisher: Arc<Box<dyn MessagePublisher + Sync + Send>>,
        system: System,
    ) -> Self {
        Self {
            system,
            decision_store,
            decision_publisher,
            decision_outbox_channel_tx,
            decision_outbox_channel_rx,
        }
    }

    pub async fn save_and_publish_task(
        publisher: Arc<Box<dyn MessagePublisher + Send + Sync>>,
        datastore: Arc<Box<dyn DecisionStore<Decision = DecisionMessage> + Send + Sync>>,
        outbox_tx: mpsc::Sender<DecisionOutboxChannelMessage>,
        decision_message: DecisionMessage,
    ) {
        let xid = decision_message.xid.clone();

        // Insert into XDB
        let decision = match datastore.insert_decision(xid.clone(), decision_message.clone()).await {
            Ok(decision) => {
                // if duplicated decision, we update the decision version and duplicate_version fields in DecisionMessage
                let decision_from_db = if decision.version.ne(&decision_message.version) {
                    DecisionMessage {
                        duplicate_version: Some(decision_message.version),
                        agent: decision_message.agent,
                        ..decision
                    }
                } else {
                    decision
                };
                Ok(decision_from_db)
            }
            Err(insert_error) => {
                let error = SystemServiceError {
                    kind: SystemServiceErrorKind::DBError,
                    reason: insert_error.reason,
                    data: insert_error.data,
                    service: "Decision Outbox Service".to_string(),
                };
                outbox_tx.send(DecisionOutboxChannelMessage::OutboundServiceError(error.clone())).await.unwrap();
                Err(error)
            }
        };

        if let Ok(decision) = decision {
            // Publish message to Kafka
            let decision_str = serde_json::to_string(&decision).unwrap();
            let mut decision_publish_header = HashMap::new();
            decision_publish_header.insert("messageType".to_string(), MessageVariant::Decision.to_string());

            debug!("Publishing message {}", decision.version);
            if let Err(publish_error) = publisher
                .publish_message(xid.as_str(), &decision_str, Some(decision_publish_header.clone()))
                .await
            {
                outbox_tx
                    .send(DecisionOutboxChannelMessage::OutboundServiceError(SystemServiceError {
                        kind: SystemServiceErrorKind::MessagePublishError,
                        reason: publish_error.reason,
                        data: publish_error.data, //Some(format!("{:?}", decision_message)),
                        service: "Decision Outbox Service".to_string(),
                    }))
                    .await
                    .unwrap();
            }
        }
    }
}

#[async_trait]
impl SystemService for DecisionOutboxService {
    //** Initiate Shutdown
    async fn shutdown_service(&mut self) {
        debug!("Decision Outbox Service shutting down");
        self.system.is_shutdown = true;
        info!("Decision Outbox Service shutdown completed!");
    }

    fn is_shutdown(&self) -> bool {
        self.system.is_shutdown
    }

    async fn update_shutdown_flag(&mut self, flag: bool) {
        self.system.is_shutdown = flag;
    }
    async fn health_check(&self) -> bool {
        //todo Postgres HC here
        true
    }

    async fn run(&mut self) -> Result<(), SystemServiceError> {
        let mut system_channel_rx = self.system.system_notifier.subscribe();
        // while !self.is_shutdown() {
        let result = tokio::select! {
            // ** Outbound message issues
            outbound_msg = self.decision_outbox_channel_rx.recv() => {
                return match outbound_msg {
                    Some(DecisionOutboxChannelMessage::Decision(decision_message)) => {
                        tokio::spawn({
                            let publisher = Arc::clone(&self.decision_publisher);
                            let datastore = Arc::clone(&self.decision_store);
                            let outbox_tx = self.decision_outbox_channel_tx.clone();
                            async move {
                                let _result = DecisionOutboxService::save_and_publish_task(publisher, datastore, outbox_tx, decision_message).await;
                            }
                        });
                        // self.save_and_publish(decision_message ).await?;
                        Ok(())
                    },

                    Some(DecisionOutboxChannelMessage::OutboundServiceError(e)) => {
                        error!("Outbound error message received... {e:#?}");
                        self.shutdown_service().await;
                        return Err(e)

                    },
                    _ => Ok(()),
                }

            }
          // ** Received System Messages (shutdown/healthcheck).
          msg = system_channel_rx.recv() => {
            let message = msg.unwrap();

             match message {

              SystemMessage::Shutdown => {
                info!("[Decision Outbox Service] Shutdown received");
                self.shutdown_service().await;
              },
              SystemMessage::HealthCheck => {
                // info!("Health Check message received <3 <3 <3");
                let is_healthy = self.health_check().await;
                self.system.system_notifier.send(SystemMessage::HealthCheckStatus { service: "Decision_Outbox", healthy: is_healthy },).unwrap();
              },
              _ => ()
           }

           Ok(())

          }
        };
        // }

        return result;
    }
}
