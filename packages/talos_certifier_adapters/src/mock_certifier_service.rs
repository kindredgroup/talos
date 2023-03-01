use async_trait::async_trait;
use talos_certifier::core::{DecisionOutboxChannelMessage, SystemService};
use talos_certifier::errors::{SystemServiceError, SystemServiceErrorKind};
use talos_certifier::model::{Decision, DecisionMessage};
use talos_certifier::ports::common::SharedPortTraits;
use talos_certifier::ChannelMessage;
use tokio::sync::mpsc;

pub struct MockCertifierService {
    pub message_channel_rx: mpsc::Receiver<ChannelMessage>,
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
    async fn shutdown_service(&mut self) {}
    fn is_shutdown(&self) -> bool {
        false
    }

    async fn update_shutdown_flag(&mut self, _flag: bool) {}
    async fn health_check(&self) -> bool {
        true
    }

    async fn run(&mut self) -> Result<(), SystemServiceError> {
        // while !self.is_shutdown() {
        tokio::select! {
           channel_msg =  self.message_channel_rx.recv() =>  {
                match channel_msg {
                    Some(ChannelMessage::Candidate( message)) => {
                        let decision_message = DecisionMessage {
                            version: message.version,
                            decision: Decision::Committed,
                            agent: message.agent,
                            cohort: message.cohort,
                            xid: message.xid,
                            suffix_start: 0,
                            safepoint: Some(0),
                            conflicts: None,
                            duplicate_version: None,
                        };
                    self.decision_outbox_tx
            .send(DecisionOutboxChannelMessage::Decision(decision_message.clone()))
            .await
            .map_err(|e| SystemServiceError {
                kind: SystemServiceErrorKind::CertifierError,
                data: Some(format!("{:?}", decision_message)),
                reason: e.to_string(),
                service: "Certifier Service".to_string(),
            })?;

                    },

                    Some(ChannelMessage::Decision(_version, _decision_message)) => {
                        // ignore decision
                    },

                    None => (),
                }
            }

        }

        Ok(())
    }
}
