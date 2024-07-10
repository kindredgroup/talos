use ahash::{HashMap, HashMapExt};
use async_trait::async_trait;
use talos_certifier::core::{DecisionOutboxChannelMessage, ServiceResult, SystemService};
use talos_certifier::errors::{SystemServiceError, SystemServiceErrorKind};
use talos_certifier::model::metrics::TxProcessingTimeline;
use talos_certifier::model::{Decision, DecisionMessage};
use talos_certifier::ports::common::SharedPortTraits;
use talos_certifier::services::MetricsServiceMessage;
use talos_certifier::ChannelMessage;
use time::OffsetDateTime;
use tokio::sync::mpsc;

pub struct MockCertifierService {
    pub message_channel_rx: mpsc::Receiver<ChannelMessage>,
    pub decision_outbox_tx: mpsc::Sender<DecisionOutboxChannelMessage>,
    pub metrics_tx: mpsc::Sender<MetricsServiceMessage>,
}

#[async_trait]
impl SharedPortTraits for MockCertifierService {
    async fn is_healthy(&self) -> bool {
        true
    }

    async fn shutdown(&self) -> bool {
        true
    }
}

#[async_trait]
impl SystemService for MockCertifierService {
    async fn run(&mut self) -> ServiceResult {
        // while !self.is_shutdown() {
        tokio::select! {
           channel_msg =  self.message_channel_rx.recv() =>  {
                match channel_msg {
                    Some(ChannelMessage::Candidate(candidate)) => {
                        let message = candidate.message;
                        let mut decision_message = DecisionMessage {
                            version: message.version,
                            decision: Decision::Committed,
                            agent: message.agent,
                            cohort: message.cohort,
                            xid: message.xid,
                            suffix_start: 0,
                            safepoint: Some(0),
                            conflict_version: None,
                            duplicate_version: None,
                            metrics: TxProcessingTimeline::default(),
                        };
                        let now = OffsetDateTime::now_utc().unix_timestamp_nanos();
                        decision_message.metrics.decision_created_at = now;

                        let decision_outbox_channel_message = DecisionOutboxChannelMessage{ message: decision_message.clone(), headers:HashMap::new() };
                        self.decision_outbox_tx
                            .send(decision_outbox_channel_message)
                            .await
                            .map_err(|e| SystemServiceError {
                                kind: SystemServiceErrorKind::CertifierError,
                                data: Some(format!("{:?}", decision_message)),
                                reason: e.to_string(),
                                service: "Certifier Service".to_string(),
                            })?;

                        let metrics_tx_cloned_2 = self.metrics_tx.clone();
                        let received_at = message.received_at;
                        let now_1 = now;
                        tokio::spawn(async move {
                            let _ = metrics_tx_cloned_2
                                .send(MetricsServiceMessage::Record(
                                    "channel_consumer_to_candidate_process_decision".to_string(),
                                    (now_1 - received_at) as u64 / 1_000_000_u64,
                                ))
                                .await;
                        });

                    },

                    Some(ChannelMessage::Decision(_)) => {
                        // ignore decision
                    },

                    None => (),
                }
            }

        }

        Ok(())
    }
}
