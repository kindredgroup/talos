use ahash::{HashMap, HashMapExt};
use async_trait::async_trait;
use talos_certifier::core::{DecisionOutboxChannelMessage, ServiceResult, SystemService};
use talos_certifier::errors::{SystemServiceError, SystemServiceErrorKind};
use talos_certifier::model::metrics::TxProcessingTimeline;
use talos_certifier::model::{CandidateMessage, Decision, DecisionMessage};
use talos_certifier::ports::common::SharedPortTraits;
use talos_certifier::ChannelMessage;
use tokio::sync::mpsc;

pub struct MockCertifierService {
    pub message_channel_rx: mpsc::Receiver<ChannelMessage<CandidateMessage>>,
    pub decision_outbox_tx: mpsc::Sender<DecisionOutboxChannelMessage>,
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
                        let decision_message = DecisionMessage {
                            version: message.version,
                            decision: Decision::Committed,
                            agent: message.agent,
                            cohort: message.cohort,
                            time: Some("2021-08-02T11:21:34.523Z".to_owned()),
                            xid: message.xid,
                            suffix_start: 0,
                            safepoint: Some(0),
                            conflict_version: None,
                            duplicate_version: None,
                            metrics: TxProcessingTimeline::default(),
                        };
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

                    },

                    Some(ChannelMessage::Decision(_)) => {
                        // ignore decision
                    },
                    Some(ChannelMessage::Reset) => {
                        // ignore reset
                    },

                    None => (),
                }
            }

        }

        Ok(())
    }
}
