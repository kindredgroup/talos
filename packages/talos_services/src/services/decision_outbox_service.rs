use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use dyn_clone::clone_box;
use log::{debug, error, info};
use talos_core::{
    core::MessageVariant,
    errors::SystemServiceError,
    model::decision_message::DecisionMessage,
    ports::{decision_store::DecisionStore, message::MessagePublisher},
    SystemMessage,
};
use tokio::sync::mpsc;

use crate::core::{DecisionOutboxChannelMessage, System, SystemService};

pub struct DecisionOutboxService {
    pub system: System,
    pub decision_outbox_channel_tx: mpsc::Sender<DecisionOutboxChannelMessage>,
    pub decision_outbox_channel_rx: mpsc::Receiver<DecisionOutboxChannelMessage>,
    pub decision_store: Box<dyn DecisionStore<Decision = DecisionMessage> + Sync + Send>,
    pub decision_publisher: Arc<Box<dyn MessagePublisher + Sync + Send>>,
}

impl DecisionOutboxService {
    pub fn new(
        decision_outbox_channel_tx: mpsc::Sender<DecisionOutboxChannelMessage>,
        decision_outbox_channel_rx: mpsc::Receiver<DecisionOutboxChannelMessage>,
        decision_store: Box<dyn DecisionStore<Decision = DecisionMessage> + Sync + Send>,
        decision_publisher: Box<dyn MessagePublisher + Sync + Send>,
        system: System,
    ) -> Self {
        Self {
            system,
            decision_store,
            decision_publisher: Arc::new(decision_publisher),
            decision_outbox_channel_tx,
            decision_outbox_channel_rx,
        }
    }

    pub async fn save_and_publish(&self, decision_message: DecisionMessage) -> Result<(), SystemServiceError> {
        let publisher = Arc::clone(&self.decision_publisher);
        let mut datastore = clone_box(&*self.decision_store);
        let outbox_tx = self.decision_outbox_channel_tx.clone();
        let mut system_channel_rx = self.system.system_notifier.subscribe();

        let handle = tokio::spawn(async move {
            // Insert into XDB

            // |||||||||||||||| TODO |||||||||||||||
            // Insert Decision into XDB
            // |||||||||||||||||||||||||||||||||||||
            let xid = decision_message.xid.clone();

            if let Err(insert_decision_error) = datastore.insert_decision(xid.clone(), decision_message.clone()).await {
                error!("Error while inserting decisiong to database ... {:#?}", insert_decision_error);

                // Todo - GK - Handle error instead of unwrap
                let result = outbox_tx
                    .send(DecisionOutboxChannelMessage::OutboundServiceError(SystemServiceError {
                        kind: talos_core::errors::SystemServiceErrorKind::DBError,
                        reason: insert_decision_error.reason,
                        data: insert_decision_error.data,
                        service: "Decision Outbox Service".to_string(),
                    }))
                    .await;

                if let Err(e) = result {
                    panic!("Error {e}")
                }
            } else {
                // Publish message to Kafka
                let decision_str = serde_json::to_string(&decision_message).unwrap();
                let mut decision_publish_header = HashMap::new();
                decision_publish_header.insert("messageType".to_string(), MessageVariant::Decision.to_string());

                debug!("Publishing message {}", decision_message.version);
                if let Err(publish_error) = publisher
                    .publish_message(xid.as_str(), &decision_str, Some(decision_publish_header.clone()))
                    .await
                {
                    // Todo - GK - Handle error instead of unwrap
                    let result = outbox_tx
                        .send(DecisionOutboxChannelMessage::OutboundServiceError(SystemServiceError {
                            kind: talos_core::errors::SystemServiceErrorKind::MessagePublishError,
                            reason: publish_error.reason,
                            data: publish_error.data, //Some(format!("{:?}", decision_message)),
                            service: "Decision Outbox Service".to_string(),
                        }))
                        .await;

                    if let Err(e) = result {
                        panic!("Error {e}")
                    }
                }
            }
        });

        tokio::select! {
            res = handle => {
                res.unwrap()
            },
            msg = system_channel_rx.recv() => {
                let message = msg.unwrap();

                if message == SystemMessage::Shutdown {
                    return Ok(())
                }
            }

        }

        Ok(())
    }

    pub async fn save_and_publish_task(
        publisher: Arc<Box<dyn MessagePublisher + Send + Sync>>,
        mut datastore: Box<dyn DecisionStore<Decision = DecisionMessage> + Send + Sync>,
        outbox_tx: mpsc::Sender<DecisionOutboxChannelMessage>,
        decision_message: DecisionMessage,
    ) -> Result<(), SystemServiceError> {
        // Insert into XDB

        // |||||||||||||||| TODO |||||||||||||||
        // Insert Decision into XDB
        // |||||||||||||||||||||||||||||||||||||
        let xid = decision_message.xid.clone();

        if let Err(insert_decision_error) = datastore.insert_decision(xid.clone(), decision_message.clone()).await {
            error!("Error while inserting decisiong to database ... {:#?}", insert_decision_error);

            // Todo - GK - Handle error instead of unwrap
            let result = outbox_tx
                .send(DecisionOutboxChannelMessage::OutboundServiceError(SystemServiceError {
                    kind: talos_core::errors::SystemServiceErrorKind::DBError,
                    reason: insert_decision_error.reason,
                    data: insert_decision_error.data,
                    service: "Decision Outbox Service".to_string(),
                }))
                .await;

            if let Err(e) = result {
                panic!("Error {e}")
            }
        } else {
            // Publish message to Kafka
            let decision_str = serde_json::to_string(&decision_message).unwrap();
            let mut decision_publish_header = HashMap::new();
            decision_publish_header.insert("messageType".to_string(), MessageVariant::Decision.to_string());

            debug!("Publishing message {}", decision_message.version);
            if let Err(publish_error) = publisher
                .publish_message(xid.as_str(), &decision_str, Some(decision_publish_header.clone()))
                .await
            {
                // Todo - GK - Handle error instead of unwrap
                let result = outbox_tx
                    .send(DecisionOutboxChannelMessage::OutboundServiceError(SystemServiceError {
                        kind: talos_core::errors::SystemServiceErrorKind::MessagePublishError,
                        reason: publish_error.reason,
                        data: publish_error.data, //Some(format!("{:?}", decision_message)),
                        service: "Decision Outbox Service".to_string(),
                    }))
                    .await;

                if let Err(e) = result {
                    panic!("Error {e}")
                }
            }
        };

        Ok(())
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
                            let datastore = clone_box(&*self.decision_store);
                            let outbox_tx = self.decision_outbox_channel_tx.clone();
                            // let mut system_channel_rx = self.system.system_notifier.subscribe();
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