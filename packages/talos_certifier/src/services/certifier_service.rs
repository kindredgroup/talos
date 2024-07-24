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
use crate::core::SystemServiceSync;
use crate::{
    core::{DecisionOutboxChannelMessage, ServiceResult, System, SystemService},
    errors::{CertificationError, SystemErrorType, SystemServiceError, SystemServiceErrorKind},
    model::{CandidateMessage, DecisionMessage},
    Certifier, ChannelMessage,
};

use super::MetricsServiceMessage;

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
    pub candidate_message_channel_rx: mpsc::Receiver<ChannelMessage>,
    pub decision_message_channel_rx: mpsc::Receiver<ChannelMessage>,
    pub commit_offset: Arc<AtomicI64>,
    pub decision_outbox_tx: mpsc::Sender<DecisionOutboxChannelMessage>,
    pub config: CertifierServiceConfig,
    pub metrics_tx: mpsc::Sender<MetricsServiceMessage>,
    // pub rt_handle: Handle,
    // pub metrics_tx: mpsc::Sender<MetricsServiceMessage>,
}

impl CertifierService {
    pub fn new(
        candidate_message_channel_rx: mpsc::Receiver<ChannelMessage>,
        decision_message_channel_rx: mpsc::Receiver<ChannelMessage>,
        decision_outbox_tx: mpsc::Sender<DecisionOutboxChannelMessage>,
        commit_offset: Arc<AtomicI64>,
        system: System,
        config: Option<CertifierServiceConfig>,
        metrics_tx: mpsc::Sender<MetricsServiceMessage>,
        // rt_handle: Handle,
    ) -> Self {
        let certifier = Certifier::new();

        let config = config.unwrap_or_default();

        let suffix = Suffix::with_config(config.suffix_config.clone());

        Self {
            suffix,
            certifier,
            system,
            candidate_message_channel_rx,
            decision_message_channel_rx,
            decision_outbox_tx,
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
    pub(crate) fn process_candidate(&mut self, message: &CandidateMessage) -> Result<DecisionMessage, CertificationError> {
        let start_candidate = Instant::now();

        let metrics_tx_cloned_1 = self.metrics_tx.clone();
        let channel_m2c_time = ((OffsetDateTime::now_utc().unix_timestamp_nanos() - message.received_at) / 1_000) as u64;

        // self.rt_handle.spawn(async move {
        //     let _ = metrics_tx_cloned_1
        //         .send(MetricsServiceMessage::Record("CHANNEL - M2C (CM) (µs)".to_string(), channel_m2c_time))
        //         .await;
        // });

        debug!("[Process Candidate message] Version {} ", message.version);

        let can_process_start = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let start_suffix_insert = Instant::now();
        // Insert into Suffix
        if message.version > 0 {
            self.suffix.insert(message.version, message.clone()).map_err(CertificationError::SuffixError)?;
        }
        let suffix_head = self.suffix.meta.head;
        let start_suffix_insert = start_suffix_insert.elapsed().as_micros() as u64;

        // let metrics_tx_cloned_1 = self.metrics_tx.clone();
        // self.rt_handle.spawn(async move {
        //     let _ = metrics_tx_cloned_1
        //         .send(MetricsServiceMessage::Record("SUFFIX_insert (µs)".to_string(), start_suffix_insert))
        //         .await;
        // });

        // Get certifier outcome
        let start_certification = Instant::now();

        let outcome = self
            .certifier
            .certify_transaction(suffix_head, message.convert_into_certifier_candidate(message.version));

        // let start_certification = start_certification.elapsed().as_micros() as u64;

        // let outcome_metrics = outcome.get_metrics();
        // let outcome_metrics_certify = outcome_metrics.certify_time;
        // let outcome_metrics_safepoint_calc_time = outcome_metrics.safepoint_calc_time;
        // let outcome_metrics_update_hashmap_time = outcome_metrics.update_hashmap_time;

        // let metrics_tx_cloned_1 = self.metrics_tx.clone();
        // self.rt_handle.spawn(async move {
        //     let _ = metrics_tx_cloned_1
        //         .send(MetricsServiceMessage::Record(
        //             "CERTIFICATION - CERTIFY (µs)".to_string(),
        //             outcome_metrics_certify / 1_000,
        //         ))
        //         .await;
        // });
        // let metrics_tx_cloned_1 = self.metrics_tx.clone();
        // self.rt_handle.spawn(async move {
        //     let _ = metrics_tx_cloned_1
        //         .send(MetricsServiceMessage::Record(
        //             "CERTIFICATION - SAFEPOINT CALC (µs)".to_string(),
        //             outcome_metrics_safepoint_calc_time / 1_000,
        //         ))
        //         .await;
        // });
        // let metrics_tx_cloned_1 = self.metrics_tx.clone();
        // self.rt_handle.spawn(async move {
        //     let _ = metrics_tx_cloned_1
        //         .send(MetricsServiceMessage::Record(
        //             "CERTIFICATION - UPDATE HASHMAPS (µs)".to_string(),
        //             outcome_metrics_update_hashmap_time / 1_000,
        //         ))
        //         .await;
        // });
        // let metrics_tx_cloned_1 = self.metrics_tx.clone();
        // self.rt_handle.spawn(async move {
        //     let _ = metrics_tx_cloned_1
        //         .send(MetricsServiceMessage::Record("CERTIFICATION (µs)".to_string(), start_certification))
        //         .await;
        // });

        // Create the Decision Message
        let mut dm = DecisionMessage::new(message, outcome, suffix_head);
        let now = OffsetDateTime::now_utc().unix_timestamp_nanos();
        dm.metrics.candidate_published = message.published_at;
        dm.metrics.candidate_received = message.received_at;
        dm.metrics.candidate_processing_started = can_process_start;
        dm.metrics.decision_created_at = now;

        // let metrics_tx_cloned_1 = self.metrics_tx.clone();
        // let pub_to_cons_diff_ms = ((dm.metrics.candidate_received - dm.metrics.candidate_published) / 1_000) as u64;
        // self.rt_handle.spawn(async move {
        //     let _ = metrics_tx_cloned_1
        //         .send(MetricsServiceMessage::Record(
        //             "Msg_publish_to_certifier_consumer (µs)".to_string(),
        //             pub_to_cons_diff_ms,
        //         ))
        //         .await;
        // });

        // let metrics_tx_cloned_1 = self.metrics_tx.clone();
        // let start_candidate = start_candidate.elapsed().as_micros() as u64;
        // self.rt_handle.spawn(async move {
        //     let _ = metrics_tx_cloned_1
        //         .send(MetricsServiceMessage::Record("process_candidate_fn (µs)".to_string(), start_candidate))
        //         .await;
        // });

        // GK: Adding process decision into process candidate
        // let _ = self.process_decision(999, &dm);

        // let metrics_tx_cloned_2 = self.metrics_tx.clone();
        // let received_at = message.received_at;
        // let now_1 = now;
        // self.rt_handle.spawn(async move {
        //     let _ = metrics_tx_cloned_2
        //         .send(MetricsServiceMessage::Record(
        //             "CHANNEL - consumer_to_candidate_process_decision (µs)".to_string(),
        //             (now_1 - received_at) as u64 / 1_000_u64,
        //         ))
        //         .await;
        // });

        Ok(dm)
    }

    pub(crate) fn process_decision(&mut self, decision_version: u64, decision_message: &DecisionMessage) -> Result<(), CertificationError> {
        // let start_decision = Instant::now();

        // let metrics_tx_cloned_1 = self.metrics_tx.clone();
        // let channel_m2c_time = ((OffsetDateTime::now_utc().unix_timestamp_nanos() - decision_message.metrics.decision_received_at) / 1_000) as u64;

        // self.rt_handle.spawn(async move {
        //     let _ = metrics_tx_cloned_1
        //         .send(MetricsServiceMessage::Record("CHANNEL - M2C (DM) (µs)".to_string(), channel_m2c_time))
        //         .await;
        // });

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
                // let start_pruning = Instant::now();

                // let end_get_safe_prune_index = start_get_safe_prune_index.elapsed().as_millis();

                // let suffix_head_before_prune = self.suffix.meta.head;
                // let suffix_prune_index_before_prune = self.suffix.meta.prune_index;
                // let suffix_length_before_prune = self.suffix.messages.len();

                // let start_prune_till_index = Instant::now();
                let pruned_suffix_items = self.suffix.prune_till_index(prune_index).unwrap();
                // let end_prune_till_index = start_prune_till_index.elapsed().as_millis();

                // let start_generate_certifier_sets_from_suffix = Instant::now();
                let pruned_items = get_nonempty_suffix_items(pruned_suffix_items.iter());
                let (readset, writeset) = generate_certifier_sets_from_suffix(pruned_items);
                // let end_generate_certifier_sets_from_suffix = start_generate_certifier_sets_from_suffix.elapsed().as_millis();

                // let start_prune_certifier_readset = Instant::now();
                Certifier::prune_set(&mut self.certifier.reads, &readset);
                // let end_prune_certifier_readset = start_prune_certifier_readset.elapsed().as_millis();

                // let start_prune_certifier_writeset = Instant::now();
                Certifier::prune_set(&mut self.certifier.writes, &writeset);
                // let end_prune_certifier_writeset = start_prune_certifier_writeset.elapsed().as_millis();

                // error!(
                //     "[PROCESS DECISION] Times... \n| Candidate version={} \n| Decision version={decision_version}
                //     \n| Suffix Head **BEFORE** pruning={suffix_head_before_prune} \n| Suffix prune index **BEFORE** pruning={suffix_prune_index_before_prune:?} \n| Suffix length **BEFORE** pruning={suffix_length_before_prune}
                //     \n| Suffix Head **AFTER** pruning={} \n| Suffix prune index **AFTER** pruning={:?} \n| Suffix length **AFTER** pruning={}
                //     \n| prior_items_decided={end_prior_items_decided} \n| update_prune_index={end_update_prune_index} \n| get_safe_prune_index={end_get_safe_prune_index} \n| prune_till_index={end_prune_till_index}
                //     \n| generate_certifier_sets_from_suffix={end_generate_certifier_sets_from_suffix} \n| prune_certifier_readset={end_prune_certifier_readset} \n| prune_certifier_writeset={end_prune_certifier_writeset}",
                //     decision_message.version,
                //     self.suffix.get_meta().head, self.suffix.meta.prune_index, self.suffix.messages.len()
                // )
                // let metrics_tx_cloned = self.metrics_tx.clone();
                // let start_pruning = start_pruning.elapsed().as_micros() as u64;
                // self.rt_handle.spawn(async move {
                //     let _ = metrics_tx_cloned
                //         .send(MetricsServiceMessage::Record("DECISION - Prune Suffix (µs) ".to_string(), start_pruning))
                //         .await;
                // });
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
            Some(ChannelMessage::Candidate(candidate)) => {
                let decision_message = self.process_candidate(&candidate.message)?;

                let mut headers = candidate.headers.clone();
                if let Ok(cert_time) = OffsetDateTime::now_utc().format(&Rfc3339) {
                    headers.insert("certTime".to_owned(), cert_time);
                }

                let decision_outbox_channel_message = DecisionOutboxChannelMessage {
                    message: decision_message.clone(),
                    headers,
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

        // let metrics_tx_cloned = self.metrics_tx.clone();
        // let start_process = start_process.elapsed().as_micros() as u64;
        // self.rt_handle.spawn(async move {
        //     let _ = metrics_tx_cloned
        //         .send(MetricsServiceMessage::Record("process_message_fn (µs)".to_string(), start_process))
        //         .await;
        // });

        // let metrics_tx_cloned = self.metrics_tx.clone();

        // let capacity_percentage = self.decision_outbox_tx.capacity() / self.decision_outbox_tx.max_capacity() * 100;

        // let metrics_tx_cloned = self.metrics_tx.clone();

        // self.rt_handle.spawn(async move {
        //     let _ = metrics_tx_cloned
        //         .send(MetricsServiceMessage::Record(
        //             "CHANNEL - C2DO capacity (%)".to_string(),
        //             capacity_percentage as u64,
        //         ))
        //         .await;
        // });

        Ok(())
    }
}

#[async_trait]
impl SystemService for CertifierService {
    async fn run(&mut self) -> ServiceResult {
        // let channel_msg = self.candidate_message_channel_rx.recv().await;
        // Ok(self.process_message(&channel_msg).await?)

        tokio::select! {
            biased;
            channel_msg = self.candidate_message_channel_rx.recv() => {
                // error!("<Candidate_Message> Processing");
                Ok(self.process_message(&channel_msg).await?)
            }
            decision_channel_msg = self.decision_message_channel_rx.recv() => {
                // error!("<Decision_Message> Processing");
                Ok(self.process_message(&decision_channel_msg).await?)
            }

        }
    }
}
