use std::sync::atomic::AtomicI64;
use std::sync::Arc;

use async_trait::async_trait;
use log::{debug, info};
use talos_suffix::core::SuffixConfig;
use talos_suffix::{get_nonempty_suffix_items, Suffix, SuffixTrait};
use tokio::sync::mpsc;

use crate::certifier::utils::generate_certifier_sets_from_suffix;
use crate::{
    certifier::Outcome,
    core::{DecisionOutboxChannelMessage, ServiceResult, System, SystemService},
    errors::{CertificationError, SystemErrorType, SystemServiceError, SystemServiceErrorKind},
    model::{CandidateMessage, DecisionMessage},
    Certifier, ChannelMessage, SystemMessage,
};

/// Certifier service configuration
#[derive(Debug, Clone, Default)]
pub struct CertifierServiceConfig {
    /// Suffix config
    pub suffix_config: SuffixConfig,
}

pub struct CertifierService {
    pub suffix: Suffix<CandidateMessage>,
    pub certifier: Certifier,
    pub system: System,
    pub message_channel_rx: mpsc::Receiver<ChannelMessage>,
    pub commit_offset: Arc<AtomicI64>,
    pub decision_outbox_tx: mpsc::Sender<DecisionOutboxChannelMessage>,
    pub config: CertifierServiceConfig,
}

impl CertifierService {
    pub fn new(
        message_channel_rx: mpsc::Receiver<ChannelMessage>,
        decision_outbox_tx: mpsc::Sender<DecisionOutboxChannelMessage>,
        commit_offset: Arc<AtomicI64>,
        system: System,
        config: Option<CertifierServiceConfig>,
    ) -> Self {
        let certifier = Certifier::new();

        let config = config.unwrap_or_default();

        let suffix = Suffix::with_config(config.suffix_config.clone());

        Self {
            suffix,
            certifier,
            system,
            message_channel_rx,
            decision_outbox_tx,
            commit_offset,
            config,
        }
    }

    fn get_conflict_candidates(&mut self, outcome: &Outcome) -> Option<CandidateMessage> {
        match outcome {
            Outcome::Aborted { version, discord: _ } => {
                if version.is_some() {
                    if let Ok(Some(msg)) = self.suffix.get(version.unwrap()) {
                        return Some(msg.item);
                    }
                }
                None
            }
            _ => None,
        }
    }

    /// Process CandidateMessage to provide the DecisionMessage
    ///
    /// * Inserts the message into suffix.
    /// * Certify the message.
    /// * Create the DecisionMessage from the certification outcome.
    ///
    pub(crate) fn process_candidate_message_outcome(&mut self, message: &CandidateMessage) -> ServiceResult<DecisionMessage> {
        debug!("[Process Candidate message] Version {} ", message.version);

        // Insert into Suffix
        self.suffix.insert(message.version, message.clone()).map_err(CertificationError::SuffixError)?;
        let suffix_head = self.suffix.meta.head;

        // Get certifier outcome
        let outcome = self
            .certifier
            .certify_transaction(suffix_head, message.convert_into_certifier_candidate(message.version));

        // If abort get the conflict record from suffix
        let conflict_candidate: Option<CandidateMessage> = self.get_conflict_candidates(&outcome);

        // Create the Decision Message
        Ok(DecisionMessage::new(message, conflict_candidate, outcome, suffix_head))
    }

    pub(crate) async fn process_candidate(&mut self, message: &CandidateMessage) -> ServiceResult {
        let decision_message = self.process_candidate_message_outcome(message)?;

        self.decision_outbox_tx
            .send(DecisionOutboxChannelMessage::Decision(decision_message.clone()))
            .await
            .map_err(|e| SystemServiceError {
                kind: SystemServiceErrorKind::SystemError(SystemErrorType::Channel),
                data: Some(format!("{:?}", decision_message)),
                reason: e.to_string(),
                service: "Certifier Service".to_string(),
            })?;

        Ok(())
    }

    pub(crate) async fn process_decision(&mut self, decision_version: u64, decision_message: &DecisionMessage) -> ServiceResult {
        // update the decision in suffix
        debug!(
            "[Process Decision message] Version {} and Decision Message {:?} ",
            decision_version, decision_message
        );

        // Reserve space if version is beyond the suffix capacity
        //
        // Applicable in scenarios where certifier starts from a committed version
        let candidate_version = match decision_message.duplicate_version {
            Some(ver) => ver,
            None => decision_message.version,
        };

        let candidate_version_index = self.suffix.index_from_head(candidate_version);

        if candidate_version_index.is_some() && candidate_version_index.unwrap().le(&self.suffix.messages.len()) {
            self.suffix
                .update_decision_suffix_item(candidate_version, decision_version)
                .map_err(CertificationError::SuffixError)?;

            // check if all prioir items are decided.

            let all_decided = self.suffix.are_prior_items_decided(candidate_version);

            if all_decided {
                self.suffix.update_prune_index(Some(candidate_version_index.unwrap()));
            }

            // prune suffix if required?
            if let Some(prune_index) = self.suffix.get_safe_prune_index() {
                let pruned_suffix_items = self.suffix.prune_till_index(prune_index).unwrap();

                let pruned_items = get_nonempty_suffix_items(pruned_suffix_items.iter());
                let (readset, writeset) = generate_certifier_sets_from_suffix(pruned_items);

                Certifier::prune_set(&mut self.certifier.reads, &readset);
                Certifier::prune_set(&mut self.certifier.writes, &writeset);
            }
            // remove sets from certifier if pruning?

            // commit the offset if all prior suffix items have been decided?
            if all_decided {
                debug!("Prior items decided if condition with dv={}", decision_message.version);

                self.commit_offset
                    .store(candidate_version.try_into().unwrap(), std::sync::atomic::Ordering::Relaxed);
            }
        }

        Ok(())
    }
}

#[async_trait]
impl SystemService for CertifierService {
    async fn shutdown_service(&mut self) {
        debug!("Shutting down Certifier!!!");

        self.system.is_shutdown = true;
        // drop(self.decision_publisher);
        info!("Certifier Service shutdown completed!");
    }
    fn is_shutdown(&self) -> bool {
        self.system.is_shutdown
    }

    async fn update_shutdown_flag(&mut self, flag: bool) {
        info!("flag {}", flag);
        self.system.is_shutdown = flag;
    }
    async fn health_check(&self) -> bool {
        true
    }

    async fn run(&mut self) -> Result<(), SystemServiceError> {
        let mut system_channel_rx = self.system.system_notifier.subscribe();

        // while !self.is_shutdown() {
        tokio::select! {
           channel_msg =  self.message_channel_rx.recv() =>  {
                match channel_msg {
                    Some(ChannelMessage::Candidate( message)) => {
                         self.process_candidate(&message).await?

                    },

                    Some(ChannelMessage::Decision(version, decision_message)) => {
                        self.process_decision(version, &decision_message).await?
                    },

                    None => (),
                    // _ => (),
                }
            }
            // ** Received System Messages (shutdown/healthcheck).
            msg = system_channel_rx.recv() => {
                    let message = msg.unwrap();

                    match message {
                        SystemMessage::Shutdown => {
                            info!("[CERTIFIER] Shutdown received");
                            self.shutdown_service().await;
                        },
                        SystemMessage::HealthCheck => {
                            // info!("Health Check message received <3 <3 <3");
                            let is_healthy = self.health_check().await;
                            self.system.system_notifier.send(SystemMessage::HealthCheckStatus { service: "CERTIFIER_SERVICE", healthy: is_healthy },).unwrap();
                        },
                        _ => ()
                }

            }


        }

        Ok(())
    }
}
