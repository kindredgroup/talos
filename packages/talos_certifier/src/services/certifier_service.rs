use std::sync::atomic::AtomicI64;
use std::sync::Arc;

use async_trait::async_trait;
use log::{debug, error, info, warn};
use talos_suffix::core::{SuffixConfig, SuffixMetricsConfig};
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

use opentelemetry::global;
use opentelemetry_otlp::WithExportConfig;
use strum::Display;
use thiserror::Error as ThisError;

/// Certifier service configuration
#[derive(Debug, Clone, Default)]
pub struct CertifierServiceConfig {
    /// Suffix config
    pub suffix_config: SuffixConfig,
    pub otel_grpc_endpoint: Option<String>,
}

pub struct CertifierService {
    pub suffix: Suffix<CandidateMessage>,
    pub certifier: Certifier,
    pub system: System,
    pub message_channel_rx: mpsc::Receiver<ChannelMessage<CandidateMessage>>,
    pub commit_offset: Arc<AtomicI64>,
    pub decision_outbox_tx: mpsc::Sender<DecisionOutboxChannelMessage>,
    pub config: CertifierServiceConfig,
}

#[derive(Debug, ThisError)]
#[error("Error initialising OTEL telemetry: '{kind}'.\nReason: {reason}\nCause: {cause:?}")]
pub struct OtelInitError {
    pub kind: InitErrorType,
    pub reason: String,
    pub cause: Option<String>,
}

#[derive(Debug, Display, PartialEq, Clone)]
pub enum InitErrorType {
    MetricError,
}

pub fn init_otel_metrics(grpc_endpoint: Option<String>) -> Result<(), OtelInitError> {
    if let Some(grpc_endpoint) = grpc_endpoint {
        let otel_exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_endpoint(grpc_endpoint)
            .with_protocol(opentelemetry_otlp::Protocol::Grpc)
            .build()
            .map_err(|metric_error| OtelInitError {
                kind: InitErrorType::MetricError,
                reason: "Unable to initialise metrics exporter".into(),
                cause: Some(format!("{:?}", metric_error)),
            })?;

        let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_periodic_exporter(otel_exporter)
            .build();

        tracing::info!("OTEL metrics provider initialised");
        global::set_meter_provider(provider);
    }

    tracing::info!("OTEL metrics initialised");

    Ok(())
}

impl CertifierService {
    pub fn new(
        message_channel_rx: mpsc::Receiver<ChannelMessage<CandidateMessage>>,
        decision_outbox_tx: mpsc::Sender<DecisionOutboxChannelMessage>,
        commit_offset: Arc<AtomicI64>,
        system: System,
        config: Option<CertifierServiceConfig>,
    ) -> Self {
        let certifier = Certifier::new();

        let config = config.unwrap_or_default();

        let suffix = if config.otel_grpc_endpoint.is_some() {
            let _ = init_otel_metrics(config.otel_grpc_endpoint.clone());
            let meter = global::meter("certifier");
            Suffix::with_config(config.suffix_config.clone(), Some((SuffixMetricsConfig { prefix: "certifier".into() }, meter)))
        } else {
            Suffix::with_config(config.suffix_config.clone(), None)
        };

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

    /// Process CandidateMessage to provide the DecisionMessage
    ///
    /// * Inserts the message into suffix.
    /// * Certify the message.
    /// * Create the DecisionMessage from the certification outcome.
    ///
    pub(crate) fn process_candidate(&mut self, message: &CandidateMessage) -> Result<DecisionMessage, CertificationError> {
        debug!("[Process Candidate message] Version {} ", message.version);

        let can_process_started = OffsetDateTime::now_utc().unix_timestamp_nanos();
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
        dm.metrics.certification_started = message.certification_started_at;
        dm.metrics.request_created = message.request_created_at;
        dm.metrics.candidate_published = message.published_at;
        dm.metrics.candidate_received = message.received_at;
        dm.metrics.candidate_processing_started = can_process_started;
        dm.metrics.decision_created_at = now;
        Ok(dm)
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

            let all_decided = self.suffix.are_prior_items_decided(candidate_version);

            if all_decided {
                self.suffix.update_prune_index(candidate_version_index);
            }

            let mut prune_candidate_version = None;
            if let Some(prune_index_before_pruning) = self.suffix.get_meta().prune_index {
                if let Some(Some(item)) = self.suffix.messages.get(prune_index_before_pruning) {
                    prune_candidate_version = Some(item.item_ver);
                }
            };

            // prune suffix if required?
            if let Some(prune_index) = self.suffix.get_safe_prune_index() {
                let pruned_suffix_items = self.suffix.prune_till_index(prune_index).unwrap();
                let pruned_items = get_nonempty_suffix_items(pruned_suffix_items.iter());

                let (readset, writeset) = generate_certifier_sets_from_suffix(pruned_items);

                Certifier::prune_set(&mut self.certifier.reads, &readset);
                Certifier::prune_set(&mut self.certifier.writes, &writeset);

                // prune_index returned from `get_safe_prune_index` can be a lower index due to suffix.meta.min_size_after_prune value.
                // Although we prune only till this new/lower prune_index, we know everything till the previous prune_index was already decided
                // and is therefore safe to update the prune_index version deriving from the corresponding version.
                //
                // This a tiny optimisation which helps in places where a slice is build using the prune_index as the
                // start or end of the slice boundary.
                if let Some(prune_vers) = prune_candidate_version {
                    let new_prune_index = self.suffix.index_from_head(prune_vers);
                    self.suffix.update_prune_index(new_prune_index);
                }
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

    pub async fn process_message(&mut self, channel_message: &Option<ChannelMessage<CandidateMessage>>) -> ServiceResult {
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

            Some(ChannelMessage::Reset) => {
                info!("Reset request received, resetting suffix.");
                debug!(
                    "Before suffix reset - suffix head = {} | suffix length = {} | last inserted version = {} ",
                    self.suffix.meta.head,
                    self.suffix.messages.len(),
                    self.suffix.meta.last_insert_vers
                );
                // Clear suffix.
                self.suffix.reset();
                debug!(
                    "After suffix reset - suffix head = {} | suffix length = {}",
                    self.suffix.meta.head,
                    self.suffix.messages.len()
                );
                // Clear the read and write set maps.
                self.certifier.reads.clear();
                self.certifier.writes.clear();
                Ok(())
            }

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
