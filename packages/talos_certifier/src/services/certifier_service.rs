use std::sync::Arc;
use std::{sync::atomic::AtomicI64};

use async_trait::async_trait;
use log::{debug, info};
use talos_suffix::{Suffix, SuffixTrait};
use tokio::sync::mpsc;

use crate::{
    certifier::Outcome,
    core::{DecisionOutboxChannelMessage, ServiceResult, System, SystemService},
    errors::{CertificationError, SystemErrorType, SystemServiceError, SystemServiceErrorKind},
    model::{CandidateMessage, DecisionMessage},
    Certifier, ChannelMessage, SystemMessage,
};

/// Certifier service configuration
pub struct CertifierServiceConfig {
    /// Initial size of the suffix
    pub suffix_size: usize,
    /// Minimum size of the suffix to maintain on pruning.
    pub min_suffix_size: usize,
    /// The frequency in milliseconds at which the check is done to decided whether the suffix should be pruned.
    pub suffix_prune_frequency_ms: u64,
}

impl Default for CertifierServiceConfig {
    fn default() -> Self {
        Self {
            suffix_size: 100_000,
            min_suffix_size: 50_000,
            suffix_prune_frequency_ms: 15_000,
        }
    }
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

        let suffix = Suffix::new(config.min_suffix_size, config.suffix_size);

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

    pub(crate) fn is_suffix_prune_ready(&mut self) -> bool {
        self.suffix.is_ready_for_prune() //.is_some()
    }

    /// Process CandidateMessage to provide the DecisionMessage
    ///
    /// * Inserts the message into suffix.
    /// * Certify the message.
    /// * Create the DecisionMessage from the certification outcome.
    ///
    pub(crate) fn process_candidate_message_outcome(&mut self, message: &CandidateMessage) -> ServiceResult<DecisionMessage> {
        info!("[Process Candidate message] Version {} ", message.version);

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
        info!("[Process Decision message] Version {} and Decision Message {:?} ", decision_version, decision_message);

        // 1. Is h<= CVi <= ml -1

        // Reserve space if version is beyond the suffix capacity
        //
        // Applicable in scenarios where certifier starts from a committed version
        let candidate_version = decision_message.version;

        let candidate_version_index = self.suffix.index_from_head(candidate_version);
        if candidate_version_index.is_some() && candidate_version_index.unwrap().le(&self.suffix.messages.len()) {
            info!(
                "I reached here.... \nis prune ready {}\nprior items are decided={} \n message len={} and prune ready index={:?} \n HEAD={}, current candidate message version={candidate_version} and index={candidate_version_index:?} and decision message version={decision_version}\n config.min_suffix_size={}",
                self.is_suffix_prune_ready(),
                self.suffix.are_prior_items_decided(candidate_version),
                self.suffix.messages.len(),
                self.suffix.meta.prune_index,
                self.suffix.meta.head,
                self.config.min_suffix_size
    
            );
            

            self.suffix
                .update_decision_suffix_item(decision_message.version, decision_version)
                .map_err(CertificationError::SuffixError)?;

            // check if all prioir items are decided.
            
            let all_decided = self.suffix.are_prior_items_decided(candidate_version);


            if all_decided {
                self.suffix.update_prune_index(Some(candidate_version_index.unwrap()));
            }

            // prune suffix if required?
            if self.is_suffix_prune_ready() {
               
                self.suffix.prune().unwrap();
                info!(
                    "Suffix pruned and new length is ... {} and head is {}!!!",
                    self.suffix.messages.len(),
                    self.suffix.meta.head
                );
            }
            // remove sets from certifier if pruning?

            // commit the offset if all prior suffix items have been decided?
            if all_decided{
                debug!("Prior items decided if condition with dv={}", decision_message.version);
                // self.suffix.update_prune_vers(Some(decision_message.version));

                self.commit_offset
                    .store(decision_version.try_into().unwrap(), std::sync::atomic::Ordering::Relaxed);
            } else {
                let k: Vec<u64> = self
                    .suffix
                    .messages
                    .range(0..candidate_version_index.unwrap())
                    .filter(|&x| x.is_some())
                    .filter_map(|x| {
                        let v = x.as_ref().unwrap();
                        v.decision_ver.is_none().then(|| v.item.version)
                    })
                    .collect();
                info!("Items not decided prioir to {candidate_version} with index={candidate_version_index:?} are \n{k:?}");

            }
            let k = self.suffix.retrieve_all_some_vec_items();
            info!("[Process Decision Message] - Entire suffix with (index, ver, decision_ver) \n{k:?}");
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
