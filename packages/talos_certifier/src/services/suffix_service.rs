use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use futures::executor::block_on;
use log::{debug, error, warn};
use talos_suffix::core::SuffixConfig;
use talos_suffix::{get_nonempty_suffix_items, Suffix, SuffixTrait};
use time::format_description::well_known::Rfc3339;
use time::{error, OffsetDateTime};
use tokio::runtime::Handle;
use tokio::sync::mpsc;

use crate::certifier::utils::generate_certifier_sets_from_suffix;
use crate::core::{CandidateChannelMessage, CertifierChannelMessage, SystemServiceSync};
use crate::{
    core::{DecisionOutboxChannelMessage, ServiceResult, System, SystemService},
    errors::{CertificationError, SystemErrorType, SystemServiceError, SystemServiceErrorKind},
    model::{CandidateMessage, DecisionMessage},
    Certifier, ChannelMessage,
};

use super::MetricsServiceMessage;

/// Certifier service configuration
#[derive(Debug, Clone, Default)]
pub struct SuffixServiceConfig {
    /// Suffix config
    pub suffix_config: SuffixConfig,
}

pub struct SuffixService {
    pub suffix: Suffix<CandidateMessage>,
    pub system: System,
    pub message_channel_rx: mpsc::Receiver<ChannelMessage>,
    pub commit_offset: Arc<AtomicI64>,
    pub certifier_tx: mpsc::Sender<CertifierChannelMessage>,
    pub config: SuffixServiceConfig,
    pub metrics_tx: mpsc::Sender<MetricsServiceMessage>,
    // pub rt_handle: Handle,
    // pub metrics_tx: mpsc::Sender<MetricsServiceMessage>,
}

impl SuffixService {
    pub fn new(
        message_channel_rx: mpsc::Receiver<ChannelMessage>,
        certifier_tx: mpsc::Sender<CertifierChannelMessage>,
        commit_offset: Arc<AtomicI64>,
        system: System,
        config: Option<SuffixServiceConfig>,
        metrics_tx: mpsc::Sender<MetricsServiceMessage>,
        // rt_handle: Handle,
    ) -> Self {
        let config = config.unwrap_or_default();

        let suffix = Suffix::with_config(config.suffix_config.clone());

        Self {
            suffix,
            system,
            message_channel_rx,
            certifier_tx,
            commit_offset,
            config,
            metrics_tx,
            // rt_handle,
        }
    }

    /// Process CandidateMessage to provide the DecisionMessage
    ///
    /// * Inserts the message into suffix.
    /// * Certify the message.
    /// * Create the DecisionMessage from the certification outcome.
    ///
    pub(crate) fn process_candidate(&mut self, message: CandidateChannelMessage) -> ServiceResult {
        let candidate = message.message.clone();
        let start_candidate = Instant::now();

        let metrics_tx_cloned_1 = self.metrics_tx.clone();
        let channel_m2c_time = ((OffsetDateTime::now_utc().unix_timestamp_nanos() - candidate.received_at) / 1_000) as u64;

        debug!("[Process Candidate message] Version {} ", candidate.version);

        let can_process_start = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let start_suffix_insert = Instant::now();
        // Insert into Suffix
        if candidate.version > 0 {
            self.suffix
                .insert(candidate.version, candidate.clone())
                .map_err(CertificationError::SuffixError)?;
        }
        let suffix_head = self.suffix.meta.head;
        let start_suffix_insert = start_suffix_insert.elapsed().as_micros() as u64;

        let certify_message = CertifierChannelMessage::Certify(suffix_head, Box::new(message.clone()));

        let certifier_tx = self.certifier_tx.clone();
        let candidate_version = candidate.version;
        tokio::spawn(async move {
            certifier_tx
                .send(certify_message)
                .await
                .map_err(|e| SystemServiceError {
                    kind: SystemServiceErrorKind::SystemError(SystemErrorType::Channel),
                    data: Some(format!("{:?}", candidate_version)),
                    reason: e.to_string(),
                    service: "Certifier Service".to_string(),
                })
                .unwrap();
        });

        Ok(())
    }

    pub(crate) fn process_decision(&mut self, decision_version: u64, decision_message: &DecisionMessage) -> Result<(), CertificationError> {
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
                .update_decision_suffix_item(candidate_version, decision_version)
                .map_err(CertificationError::SuffixError)?;

            // check if all prioir items are decided.

            // let start_prior_items_decided = Instant::now();
            let all_decided = self.suffix.are_prior_items_decided(candidate_version);
            // let end_prior_items_decided = start_prior_items_decided.elapsed().as_millis();

            // let start_update_prune_index = Instant::now();
            if all_decided {
                self.suffix.update_prune_index(candidate_version_index);
            }
            // let end_update_prune_index = start_update_prune_index.elapsed().as_millis();

            // prune suffix if required?
            // let start_get_safe_prune_index = Instant::now();

            if let Some(prune_index) = self.suffix.get_safe_prune_index() {
                // let start_prune_till_index = Instant::now();
                let pruned_suffix_items = self.suffix.prune_till_index(prune_index).unwrap();
                // let end_prune_till_index = start_prune_till_index.elapsed().as_millis();

                // let start_generate_certifier_sets_from_suffix = Instant::now();
                let pruned_items = get_nonempty_suffix_items(pruned_suffix_items.iter());
                let (readset, writeset) = generate_certifier_sets_from_suffix(pruned_items);
                // let end_generate_certifier_sets_from_suffix = start_generate_certifier_sets_from_suffix.elapsed().as_millis();

                let cleanup_message = CertifierChannelMessage::Cleanup((Box::new(readset), Box::new(writeset)));
                let certifier_tx = self.certifier_tx.clone();
                tokio::spawn(async move {
                    certifier_tx
                        .send(cleanup_message)
                        .await
                        .map_err(|e| SystemServiceError {
                            kind: SystemServiceErrorKind::SystemError(SystemErrorType::Channel),
                            data: Some(format!("{:?}", candidate_version)),
                            reason: e.to_string(),
                            service: "Certifier Service".to_string(),
                        })
                        .unwrap();
                });
            };
            // remove sets from certifier if pruning?

            // commit the offset if all prior suffix items have been decided?
            if all_decided {
                debug!("Prior items decided if condition with dv={}", decision_message.version);

                self.commit_offset.store(candidate_version as i64, std::sync::atomic::Ordering::Relaxed);
            }

            // let metrics_tx_cloned = self.metrics_tx.clone();
            // let start_decision = start_decision.elapsed().as_micros() as u64;
            // self.rt_handle.spawn(async move {
            //     let _ = metrics_tx_cloned
            //         .send(MetricsServiceMessage::Record("DECISION - process_decision_fn (µs)".to_string(), start_decision))
            //         .await;
            // });
        }

        Ok(())
    }

    pub async fn process_message(&mut self, channel_message: &Option<ChannelMessage>) -> ServiceResult {
        let start_process = Instant::now();

        if let Err(certification_error) = match channel_message {
            Some(ChannelMessage::Candidate(candidate)) => self.process_candidate(*candidate.clone()),

            Some(ChannelMessage::Decision(decision)) => Ok(self.process_decision(decision.decision_version, &decision.message)?),

            None => Ok(()),
            // _ => (),
        } {
            // Ignore errors not required to cause the app to shutdown.
            match &certification_error {
                // CertificationError::SuffixError(..) => {
                //     warn!("{:?} ", certification_error.to_string());
                // }
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
impl SystemService for SuffixService {
    async fn run(&mut self) -> ServiceResult {
        let channel_msg = self.message_channel_rx.recv().await;
        Ok(self.process_message(&channel_msg).await?)
    }
}
