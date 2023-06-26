use std::sync::atomic::AtomicI64;
use std::sync::Arc;

use async_trait::async_trait;
use log::{debug, error, warn};
use talos_suffix::core::SuffixConfig;
use talos_suffix::{get_nonempty_suffix_items, Suffix, SuffixTrait};
use time::OffsetDateTime;
use tokio::sync::mpsc;

use crate::certifier::utils::generate_certifier_sets_from_suffix;
use crate::{
    certifier::Outcome,
    core::{DecisionOutboxChannelMessage, ServiceResult, System, SystemService},
    errors::{CertificationError, SystemErrorType, SystemServiceError, SystemServiceErrorKind},
    model::{CandidateMessage, DecisionMessage},
    Certifier, ChannelMessage,
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
    pub(crate) fn process_candidate(&mut self, message: &CandidateMessage) -> Result<DecisionMessage, CertificationError> {
        debug!("[Process Candidate message] Version {} ", message.version);

        let can_process_start = OffsetDateTime::now_utc().unix_timestamp_nanos();
        // Insert into Suffix
        if message.version > 0 {
            self.suffix.insert(message.version, message.clone()).map_err(CertificationError::SuffixError)?;
        }
        let suffix_head = self.suffix.meta.head;

        // Get certifier outcome
        let outcome = self
            .certifier
            .certify_transaction(suffix_head, message.convert_into_certifier_candidate(message.version));

        // If abort get the conflict record from suffix
        let conflict_candidate: Option<CandidateMessage> = self.get_conflict_candidates(&outcome);

        // Create the Decision Message
        let mut dm = DecisionMessage::new(message, conflict_candidate, outcome, suffix_head);
        dm.can_received_at = message.received_at;
        dm.can_process_start = can_process_start;
        dm.can_process_end = OffsetDateTime::now_utc().unix_timestamp_nanos();
        dm.created_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
        Ok(dm)
    }

    pub(crate) fn process_decision(&mut self, decision_version: &u64, decision_message: &DecisionMessage) -> Result<(), CertificationError> {
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

        // This has big impact on performance, even if log level is below info.
        // info!("Suffix with items : {:?}", self.suffix.retrieve_all_some_vec_items());

        if candidate_version_index.is_some() && candidate_version_index.unwrap().le(&self.suffix.messages.len()) {
            self.suffix
                .update_decision_suffix_item(candidate_version, *decision_version)
                .map_err(CertificationError::SuffixError)?;

            // check if all prioir items are decided.

            let all_decided = self.suffix.are_prior_items_decided(candidate_version);

            if all_decided {
                self.suffix.update_prune_index(candidate_version_index);
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

                self.commit_offset.store(candidate_version as i64, std::sync::atomic::Ordering::Relaxed);
            }
        }

        Ok(())
    }

    pub async fn process_message(&mut self, channel_message: &Option<ChannelMessage>) -> ServiceResult {
        if let Err(certification_error) = match channel_message {
            Some(ChannelMessage::Candidate(message)) => {
                let decision_message = self.process_candidate(message)?;

                Ok(self
                    .decision_outbox_tx
                    .send(DecisionOutboxChannelMessage::Decision(decision_message.clone()))
                    .await
                    .map_err(|e| SystemServiceError {
                        kind: SystemServiceErrorKind::SystemError(SystemErrorType::Channel),
                        data: Some(format!("{:?}", decision_message)),
                        reason: e.to_string(),
                        service: "Certifier Service".to_string(),
                    })?)
            }

            Some(ChannelMessage::Decision(version, decision_message)) => self.process_decision(version, decision_message),

            None => Ok(()),
            // _ => (),
        } {
            // Ignore errors not required to cause the app to shutdown.
            match &certification_error {
                CertificationError::SuffixError(..) => {
                    warn!("{:?} ", certification_error.to_string());
                }
                _ => {
                    // *** Shutdown the current service and return the error
                    error!("{:?} ", certification_error.to_string());
                    return Err(certification_error.into());
                }
            }
        };

        Ok(())
    }
}

#[async_trait]
impl SystemService for CertifierService {
    async fn run(&mut self) -> ServiceResult {
        let channel_msg = self.message_channel_rx.recv().await;
        Ok(self.process_message(&channel_msg).await?)
    }
}
