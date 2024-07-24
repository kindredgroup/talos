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
use crate::core::{CertifierChannelMessage, SystemServiceSync};
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
    // pub suffix_config: SuffixConfig,
}

pub struct CertifierService {
    pub certifier: Certifier,
    pub system: System,
    pub certifier_rx: mpsc::Receiver<CertifierChannelMessage>,
    pub decision_outbox_tx: mpsc::Sender<DecisionOutboxChannelMessage>,
    pub metrics_tx: mpsc::Sender<MetricsServiceMessage>,
    // pub rt_handle: Handle,
    // pub metrics_tx: mpsc::Sender<MetricsServiceMessage>,
}

impl CertifierService {
    pub fn new(
        certifier_rx: mpsc::Receiver<CertifierChannelMessage>,
        decision_outbox_tx: mpsc::Sender<DecisionOutboxChannelMessage>,
        system: System,
        config: Option<CertifierServiceConfig>,
        metrics_tx: mpsc::Sender<MetricsServiceMessage>,
        // rt_handle: Handle,
    ) -> Self {
        let certifier = Certifier::new();

        let config = config.unwrap_or_default();

        Self {
            certifier,
            system,
            certifier_rx,
            decision_outbox_tx,
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
    pub(crate) fn process_candidate(&mut self, suffix_head: u64, message: &CandidateMessage) -> Result<DecisionMessage, CertificationError> {
        // Get certifier outcome
        let start_certification = Instant::now();

        let can_process_start = OffsetDateTime::now_utc().unix_timestamp_nanos();

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

    pub(crate) fn process_cleanup(&mut self, readset: ahash::AHashMap<String, u64>, writeset: ahash::AHashMap<String, u64>) -> Result<(), CertificationError> {
        Certifier::prune_set(&mut self.certifier.reads, &readset);
        // let end_prune_certifier_readset = start_prune_certifier_readset.elapsed().as_millis();

        // let start_prune_certifier_writeset = Instant::now();
        Certifier::prune_set(&mut self.certifier.writes, &writeset);

        Ok(())
    }

    pub async fn process_message(&mut self, channel_message: &Option<CertifierChannelMessage>) -> ServiceResult {
        let start_process = Instant::now();

        if let Err(certification_error) = match channel_message {
            Some(CertifierChannelMessage::Certify(suffix_head, candidate)) => {
                let decision_message = self.process_candidate(*suffix_head, &candidate.message)?;

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

            Some(CertifierChannelMessage::Cleanup((readset, writeset))) => self.process_cleanup(*readset.clone(), *writeset.clone()),

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
        let channel_msg = self.certifier_rx.recv().await;
        Ok(self.process_message(&channel_msg).await?)
    }
}
