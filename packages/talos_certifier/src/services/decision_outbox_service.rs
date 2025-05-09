use std::sync::Arc;

use ahash::HashMap;
use async_trait::async_trait;
use log::{debug, error};

use time::OffsetDateTime;
use tokio::sync::mpsc;

use crate::core::ServiceResult;
use crate::model::decision_headers::{DecisionCertHeaders, DecisionHeaderBuilder, DecisionMetaHeaders};
use crate::model::DEFAULT_DECISION_MESSAGE_VERSION;
use crate::{
    core::{DecisionOutboxChannelMessage, System, SystemService},
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

        let started_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let mut decision = datastore
            .insert_decision(xid, decision_message.clone())
            .await
            .map_err(|insert_error| SystemServiceError {
                kind: SystemServiceErrorKind::DBError,
                reason: format!("Datastore error kind = {:?} and reason = {}", insert_error.kind, insert_error.reason),
                data: insert_error.data,
                service: "Decision Outbox Service".to_string(),
            })?;
        let finished_at = OffsetDateTime::now_utc().unix_timestamp_nanos();

        if decision.version.ne(&decision_message.version) {
            decision = DecisionMessage {
                duplicate_version: Some(decision_message.version),
                agent: decision_message.agent.clone(),
                ..decision
            }
        }

        decision.metrics.db_save_started = started_at;
        decision.metrics.db_save_ended = finished_at;
        Ok(decision)
    }

    pub async fn publish_decision(
        publisher: &Arc<Box<dyn MessagePublisher + Send + Sync>>,
        decision_message: &DecisionMessage,
        headers: HashMap<String, String>,
    ) -> ServiceResult {
        let xid = decision_message.xid.clone();
        let decision_str = serde_json::to_string(&decision_message).map_err(|e| {
            Box::new(SystemServiceError {
                kind: SystemServiceErrorKind::ParseError,
                reason: format!("Error serializing decision message to string - {}", e),
                data: Some(format!("{:?}", decision_message)),
                service: "Decision Outbox Service".to_string(),
            })
        })?;

        debug!("Publishing message {}", decision_message.version);
        publisher.publish_message(xid.as_str(), &decision_str, headers).await.map_err(|publish_error| {
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

        if let Some(decision_channel_message) = self.decision_outbox_channel_rx.recv().await {
            let DecisionOutboxChannelMessage {
                headers,
                message: decision_message,
            } = decision_channel_message;
            tokio::spawn({
                let decision_headers: DecisionHeaderBuilder<crate::model::decision_headers::MetaHeaders, crate::model::decision_headers::NoCertHeaders> =
                    DecisionHeaderBuilder::with_additional_headers(headers.into()).add_meta_headers(DecisionMetaHeaders::new(
                        DEFAULT_DECISION_MESSAGE_VERSION, // major version of decision message
                        self.system.name.clone(),
                        None,
                    ));

                async move {
                    match DecisionOutboxService::save_decision_to_xdb(&datastore, &decision_message).await {
                        Ok(decision) => {
                            if let Err(publish_error) = DecisionOutboxService::publish_decision(
                                &publisher,
                                &decision,
                                decision_headers.add_cert_headers(DecisionCertHeaders::new(&decision)).build().into(),
                            )
                            .await
                            {
                                error!(
                                    "Error publishing message for version={} with reason={:?}",
                                    decision.version,
                                    publish_error.to_string()
                                );
                            }
                        }
                        Err(db_error) => {
                            error!("Error saving decision to XDB with reason={:?}", db_error.to_string());
                            system.system_notifier.send(SystemMessage::ShutdownWithError(db_error)).unwrap();
                        }
                    };
                }
            });
        };

        Ok(())
    }
}
