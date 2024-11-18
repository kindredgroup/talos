use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use log::{debug, error, warn};
use talos_suffix::core::SuffixConfig;
use talos_suffix::{get_nonempty_suffix_items, Suffix, SuffixTrait};
use time::OffsetDateTime;
use tokio::sync::mpsc;

use crate::certifier::utils::generate_certifier_sets_from_suffix;
use crate::{
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
    // pub total_prune: (u32, u128),
    // pub total_decisions: (u32, u128, Instant),
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
            // total_decisions: (0, 0, Instant::now()),
            // total_prune: (0, 0),
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

        // Create the Decision Message
        let mut dm = DecisionMessage::new(message, outcome, suffix_head);
        let now = OffsetDateTime::now_utc().unix_timestamp_nanos();
        dm.metrics.candidate_published = message.published_at;
        dm.metrics.candidate_received = message.received_at;
        dm.metrics.candidate_processing_started = can_process_start;
        dm.metrics.decision_created_at = now;
        Ok(dm)
    }

    pub(crate) fn process_decision(&mut self, decision_version: u64, decision_message: &DecisionMessage) -> Result<(), CertificationError> {
        // update the decision in suffix
        debug!(
            "[Process Decision message] Version {} and Decision Message {:?} ",
            decision_version, decision_message
        );

        // let start_decision_timing = Instant::now();

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
                .update_decision_suffix_item(candidate_version, decision_version)
                .map_err(CertificationError::SuffixError)?;

            // check if all prioir items are decided.

            let all_decided = self.suffix.are_prior_items_decided(candidate_version);

            if all_decided {
                self.suffix.update_prune_index(candidate_version_index);
            }

            // prune suffix if required?
            if let Some(prune_index) = self.suffix.get_safe_prune_index() {
                // let start_ms = Instant::now();

                let pruned_suffix_items = self.suffix.prune_till_index(prune_index).unwrap();

                let pruned_items = get_nonempty_suffix_items(pruned_suffix_items.iter());
                let (readset, writeset) = generate_certifier_sets_from_suffix(pruned_items);

                Certifier::prune_set(&mut self.certifier.reads, &readset);
                Certifier::prune_set(&mut self.certifier.writes, &writeset);

                // let end_ns = start_ms.elapsed().as_nanos();

                // self.total_prune.0 += 1;
                // self.total_prune.1 = end_ns;

                // log::warn!(
                //     "--- Pruning suffix with prune_index={prune_index} | threshold={:?} | elapsed_ns={:?}ns \n
                //      +++ Total time taken to prune {} times = {}ns",
                //     self.suffix.get_meta().prune_start_threshold,
                //     end_ns,
                //     self.total_prune.0,
                //     self.total_prune.1
                // );
            }
            // remove sets from certifier if pruning?

            // commit the offset if all prior suffix items have been decided?
            if all_decided {
                debug!("Prior items decided if condition with dv={}", decision_message.version);

                self.commit_offset.store(candidate_version as i64, std::sync::atomic::Ordering::Relaxed);
            }
        }

        // self.total_decisions.0 += 1;
        // self.total_decisions.1 += start_decision_timing.elapsed().as_millis();

        // let time_diff = start_decision_timing.duration_since(self.total_decisions.2.clone());
        // log::warn!(
        //     "### Total time taken for {} decisions = {} ms, duration since last = {}ms",
        //     self.total_decisions.0,
        //     self.total_decisions.1,
        //     time_diff.as_millis()
        // );
        // self.total_decisions.2 = start_decision_timing;

        Ok(())
    }

    pub async fn process_message(&mut self, channel_message: &Option<ChannelMessage>) -> ServiceResult {
        if let Err(certification_error) = match channel_message {
            Some(ChannelMessage::Candidate(candidate)) => {
                let decision_message = self.process_candidate(&candidate.message)?;

                let decision_outbox_channel_message = DecisionOutboxChannelMessage {
                    message: decision_message.clone(),
                    headers: candidate.headers.clone(),
                };

                Ok(self
                    .decision_outbox_tx
                    .send(decision_outbox_channel_message)
                    .await
                    .map_err(|e| SystemServiceError {
                        kind: SystemServiceErrorKind::SystemError(SystemErrorType::Channel),
                        data: Some(format!("{:?}", decision_message)),
                        reason: e.to_string(),
                        service: "Certifier Service".to_string(),
                    })?)
            }

            Some(ChannelMessage::Decision(decision)) => self.process_decision(decision.decision_version, &decision.message),

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
