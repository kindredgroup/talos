use std::sync::Arc;
use std::time::Instant;

use ahash::{HashMap, HashMapExt};
use async_trait::async_trait;
use log::{debug, error};

use time::OffsetDateTime;
use tokio::sync::mpsc;

use crate::core::ServiceResult;
use crate::{
    core::{DecisionOutboxChannelMessage, MessageVariant, System, SystemService},
    errors::{SystemServiceError, SystemServiceErrorKind},
    model::DecisionMessage,
    ports::{DecisionStore, MessagePublisher},
    SystemMessage,
};

use super::MetricsServiceMessage;

pub struct DecisionOutboxService {
    pub system: System,
    pub decision_outbox_channel_tx: mpsc::Sender<DecisionOutboxChannelMessage>,
    pub decision_outbox_channel_rx: mpsc::Receiver<DecisionOutboxChannelMessage>,
    pub decision_store: Arc<Box<dyn DecisionStore<Decision = DecisionMessage> + Sync + Send>>,
    pub decision_publisher: Arc<Box<dyn MessagePublisher + Sync + Send>>,
    pub metrics_tx: mpsc::Sender<MetricsServiceMessage>,
}

impl DecisionOutboxService {
    pub fn new(
        decision_outbox_channel_tx: mpsc::Sender<DecisionOutboxChannelMessage>,
        decision_outbox_channel_rx: mpsc::Receiver<DecisionOutboxChannelMessage>,
        decision_store: Arc<Box<dyn DecisionStore<Decision = DecisionMessage> + Sync + Send>>,
        decision_publisher: Arc<Box<dyn MessagePublisher + Sync + Send>>,
        system: System,
        metrics_tx: mpsc::Sender<MetricsServiceMessage>,
    ) -> Self {
        Self {
            system,
            decision_store,
            decision_publisher,
            decision_outbox_channel_tx,
            decision_outbox_channel_rx,
            metrics_tx,
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
                reason: insert_error.reason,
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

        let mut decision_publish_header = headers;
        decision_publish_header.insert("messageType".to_string(), MessageVariant::Decision.to_string());
        decision_publish_header.insert("certXid".to_string(), decision_message.xid.to_owned());

        if let Some(safepoint) = decision_message.safepoint {
            decision_publish_header.insert("certSafepoint".to_string(), safepoint.to_string());
        }
        decision_publish_header.insert("certAgent".to_string(), decision_message.agent.to_owned());

        debug!("Publishing message {}", decision_message.version);
        publisher
            .publish_message(xid.as_str(), &decision_str, decision_publish_header.clone())
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

    async fn run_2(&mut self) -> ServiceResult {
        let datastore = Arc::clone(&self.decision_store);
        let publisher = Arc::clone(&self.decision_publisher);
        let system = self.system.clone();

        let buffer_size: usize = 5_000;
        let mut buffer = Vec::with_capacity(buffer_size * 2);

        let buffer_size = self.decision_outbox_channel_rx.recv_many(&mut buffer, buffer_size).await;

        if buffer_size > 0 {
            let metrics_tx_cloned_2 = self.metrics_tx.clone();
            let decision_messages = buffer
                .iter()
                .map(|decision_channel_message| {
                    let DecisionOutboxChannelMessage {
                        headers,
                        message: decision_message,
                    } = decision_channel_message;
                    let received_at = decision_message.metrics.decision_created_at; // received_at;
                    let now = OffsetDateTime::now_utc().unix_timestamp_nanos();
                    let value = metrics_tx_cloned_2.clone();
                    tokio::spawn(async move {
                        let _ = value
                            .send(MetricsServiceMessage::Record(
                                "CHANNEL - C2DO from DM Created".to_string(),
                                (now - received_at) as u64 / 1_000_u64,
                            ))
                            .await;
                    });

                    decision_message.clone()
                })
                .collect::<Vec<DecisionMessage>>();

            let metric_tx_cloned = self.metrics_tx.clone();
            let metric_tx_cloned_1 = self.metrics_tx.clone();
            tokio::spawn(async move {
                let start_db = Instant::now();
                match datastore.insert_decision_multi(decision_messages).await {
                    Ok(decisions) => {
                        for decision in decisions {
                            // let key = decision.xid;
                            let publisher_1 = publisher.clone();
                            let metric_tx_cloned_2 = metric_tx_cloned_1.clone();
                            tokio::spawn(async move {
                                let start_publish = Instant::now();

                                let headerssss = HashMap::new();
                                if let Err(publish_error) = DecisionOutboxService::publish_decision(&publisher_1, &decision, headerssss).await {
                                    error!(
                                        "Error publishing message for version={} with reason={:?}",
                                        decision.version,
                                        publish_error.to_string()
                                    );
                                }

                                let start_publish = start_publish.elapsed().as_micros() as u64;

                                let cm_received_time_ns = decision.metrics.candidate_received;
                                tokio::spawn(async move {
                                    let _ = metric_tx_cloned_2
                                        .send(MetricsServiceMessage::Record("publish_decision (µs)".to_string(), start_publish))
                                        .await;

                                    //                 let cm_publish_time_ns = decision.metrics.candidate_published;
                                    let now = OffsetDateTime::now_utc().unix_timestamp_nanos();
                                    let _ = metric_tx_cloned_2
                                        .send(MetricsServiceMessage::Record(
                                            "ROUND TRIP - Consume CM to Publish Decision (µs) ".to_string(),
                                            ((now - cm_received_time_ns) / 1_000) as u64,
                                        ))
                                        .await;
                                });
                            });
                        }
                    }
                    Err(db_error) => {
                        error!("Error inserting decision... {:#?}", db_error);
                        // system.system_notifier.send(SystemMessage::ShutdownWithError(db_error)).unwrap();
                    }
                };
                let start_db = start_db.elapsed().as_micros() as u64;

                tokio::spawn(async move {
                    let _ = metric_tx_cloned
                        .send(MetricsServiceMessage::Record("save_decision_to_xdb (µs)".to_string(), start_db))
                        .await;
                });
            });
        }

        Ok(())
    }
}

#[async_trait]
impl SystemService for DecisionOutboxService {
    async fn run(&mut self) -> ServiceResult {
        // error!("inside do service");
        let datastore = Arc::clone(&self.decision_store);
        let publisher = Arc::clone(&self.decision_publisher);
        let system = self.system.clone();

        // self.run_2().await?;
        if let Some(decision_channel_message) = self.decision_outbox_channel_rx.recv().await {
            let DecisionOutboxChannelMessage {
                headers,
                message: decision_message,
            } = decision_channel_message;

            let metrics_tx_cloned_2 = self.metrics_tx.clone();
            let received_at = decision_message.metrics.decision_created_at; // received_at;
            let now = OffsetDateTime::now_utc().unix_timestamp_nanos();
            tokio::spawn(async move {
                let _ = metrics_tx_cloned_2
                    .send(MetricsServiceMessage::Record(
                        "CHANNEL - C2DO from DM Created".to_string(),
                        (now - received_at) as u64 / 1_000_u64,
                    ))
                    .await;
            });

            let metric_tx_cloned = self.metrics_tx.clone();

            tokio::spawn(async move {
                let start_db = Instant::now();
                match DecisionOutboxService::save_decision_to_xdb(&datastore, &decision_message).await {
                    Ok(decision) => {
                        let _ = metric_tx_cloned
                            .send(MetricsServiceMessage::Record(
                                "save_decision_to_xdb (µs)".to_string(),
                                start_db.elapsed().as_micros() as u64,
                            ))
                            .await;
                        let start_publish = Instant::now();

                        if let Err(publish_error) = DecisionOutboxService::publish_decision(&publisher, &decision, headers).await {
                            error!(
                                "Error publishing message for version={} with reason={:?}",
                                decision.version,
                                publish_error.to_string()
                            );
                        }

                        let mc = metric_tx_cloned.clone();

                        let cm_received_time_ns = decision_message.metrics.candidate_received;
                        let cm_publish_time_ns = decision_message.metrics.candidate_published;
                        let now = OffsetDateTime::now_utc().unix_timestamp_nanos();
                        let _ = mc
                            .send(MetricsServiceMessage::Record(
                                "ROUND TRIP - Consume CM to Publish Decision (µs) ".to_string(),
                                ((now - cm_received_time_ns) / 1_000) as u64,
                            ))
                            .await;

                        let _ = mc
                            .send(MetricsServiceMessage::Record(
                                "ROUND TRIP - Publish CM to Publish Decision (µs) ".to_string(),
                                ((now - cm_publish_time_ns) / 1_000) as u64,
                            ))
                            .await;

                        let _ = metric_tx_cloned
                            .send(MetricsServiceMessage::Record(
                                "publish_decision (µs) ".to_string(),
                                start_publish.elapsed().as_micros() as u64,
                            ))
                            .await;
                    }
                    Err(db_error) => {
                        system.system_notifier.send(SystemMessage::ShutdownWithError(db_error)).unwrap();
                    }
                };
            });
        };

        Ok(())
    }
}
