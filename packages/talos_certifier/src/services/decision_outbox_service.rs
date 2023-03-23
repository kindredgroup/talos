use std::time::{SystemTime, UNIX_EPOCH};
use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use log::debug;

use tokio::sync::mpsc;

use crate::core::ServiceResult;
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

    pub async fn save_decision_to_xdb(
        datastore: &Arc<Box<dyn DecisionStore<Decision = DecisionMessage> + Send + Sync>>,
        decision_message: &DecisionMessage,
    ) -> ServiceResult<DecisionMessage> {
        let xid = decision_message.xid.clone();

        let mut decision = datastore
            .insert_decision(xid, decision_message.clone())
            .await
            .map_err(|insert_error| SystemServiceError {
                kind: SystemServiceErrorKind::DBError,
                reason: insert_error.reason,
                data: insert_error.data,
                service: "Decision Outbox Service".to_string(),
            })?;

        if decision.version.ne(&decision_message.version) {
            decision = DecisionMessage {
                duplicate_version: Some(decision_message.version),
                agent: decision_message.agent.clone(),
                ..decision
            }
        }

        Ok(decision)
    }

    pub async fn publish_decision(publisher: &Arc<Box<dyn MessagePublisher + Send + Sync>>, decision_message: &DecisionMessage) -> ServiceResult {
        let xid = decision_message.xid.clone();
        let decision_str = serde_json::to_string(&decision_message).unwrap();

        let mut decision_publish_header = HashMap::new();
        decision_publish_header.insert("messageType".to_string(), MessageVariant::Decision.to_string());
        decision_publish_header.insert(
            "decisionTime".to_string(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos().to_string(),
        );
        decision_publish_header.insert("certAgent".to_string(), decision_message.agent.clone());

        debug!("Publishing message {}", decision_message.version);
        publisher
            .publish_message(xid.as_str(), &decision_str, Some(decision_publish_header.clone()))
            .await
            .map_err(|publish_error| {
                Box::new(SystemServiceError {
                    kind: SystemServiceErrorKind::MessagePublishError,
                    reason: publish_error.reason,
                    data: publish_error.data, //Some(format!("{:?}", decision_message)),
                    service: "Decision Outbox Service".to_string(),
                })
            })
    }
}

#[async_trait]
impl SystemService for DecisionOutboxService {
    async fn run(&mut self) -> ServiceResult {
        let datastore = Arc::clone(&self.decision_store);
        let publisher = Arc::clone(&self.decision_publisher);
        let system = self.system.clone();

        if let Some(DecisionOutboxChannelMessage::Decision(decision_message)) = self.decision_outbox_channel_rx.recv().await {
            tokio::spawn(async move {
                match DecisionOutboxService::save_decision_to_xdb(&datastore, &decision_message).await {
                    Ok(decision) => {
                        if let Err(publish_error) = DecisionOutboxService::publish_decision(&publisher, &decision).await {
                            system.system_notifier.send(SystemMessage::ShutdownWithError(publish_error)).unwrap();
                        }
                    }
                    Err(error) => {
                        system.system_notifier.send(SystemMessage::ShutdownWithError(error)).unwrap();
                    }
                };
            });
        };

        Ok(())
    }
}
