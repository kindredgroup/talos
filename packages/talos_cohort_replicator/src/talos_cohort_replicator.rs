use std::sync::Arc;

use opentelemetry::global;
use talos_certifier::{ports::MessageReciever, ChannelMessage};
use talos_suffix::{core::SuffixConfig, Suffix};
use tokio::{sync::mpsc, task::JoinHandle, try_join};

use crate::{
    callbacks::{ReplicatorInstaller, ReplicatorSnapshotProvider},
    core::Replicator,
    errors::{ReplicatorError, ReplicatorErrorKind},
    models::{ReplicatorCandidate, ReplicatorCandidateMessage},
    otel::{
        initialiser::{init_otel_logs_tracing, init_otel_metrics},
        otel_config::ReplicatorOtelConfig,
    },
    services::{
        replicator_service::{replicator_service, ReplicatorServiceConfig},
        statemap_installer_service::{installation_service, StatemapInstallerConfig},
        statemap_queue_service::{statemap_queue_service, StatemapQueueServiceConfig},
    },
};

fn create_channel<T>(channel_size: usize) -> (tokio::sync::mpsc::Sender<T>, tokio::sync::mpsc::Receiver<T>) {
    mpsc::channel::<T>(channel_size)
}

/// Configs used by the Cohort Replicator
#[derive(Clone, Debug)]
pub struct CohortReplicatorConfig {
    /// Replicator and Installer stats. Defaults to `false`.
    pub enable_stats: bool,
    /// Size of channel used to communicate between threads. Defaults to `100_000`
    pub channel_size: usize,

    pub suffix_capacity: usize,
    pub suffix_prune_threshold: Option<usize>,
    pub suffix_minimum_size_on_prune: Option<usize>,

    pub certifier_message_receiver_commit_freq_ms: u64,

    pub statemap_queue_cleanup_freq_ms: u64,

    pub statemap_installer_threadpool: u64,

    pub otel_telemetry: ReplicatorOtelConfig,
}

async fn flatten_service_result<T>(handle: JoinHandle<Result<T, ReplicatorError>>) -> Result<T, ReplicatorError> {
    match handle.await {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(err)) => Err(err),
        Err(err) => Err(ReplicatorError {
            kind: crate::errors::ReplicatorErrorKind::Internal,
            reason: err.to_string(),
            cause: None,
        }),
    }
}

/// Entry point to replicator and statemap installer
///

pub async fn talos_cohort_replicator<M, Snap>(
    certifier_message_receiver: M,                                  //used by Replicator service
    statemap_installer: Arc<dyn ReplicatorInstaller + Send + Sync>, // Used by Statemap queue service
    snapshot_api: Snap,                                             // Used by Statemap Installer service.
    config: CohortReplicatorConfig,
) -> Result<((), (), ()), ReplicatorError>
where
    M: MessageReciever<Message = ChannelMessage<ReplicatorCandidateMessage>> + Send + Sync + 'static,
    Snap: ReplicatorSnapshotProvider + Send + Sync + 'static,
{
    if config.otel_telemetry.init_otel {
        init_otel_logs_tracing(
            config.otel_telemetry.name.clone(),
            config.otel_telemetry.enable_traces,
            config.otel_telemetry.grpc_endpoint.clone(),
            "info",
        )
        .map_err(|e| ReplicatorError {
            kind: ReplicatorErrorKind::Internal,
            reason: "Unable to initialise OTEL logs and traces for replicator".into(),
            cause: Some(format!("{:?}", e)),
        })?;

        if config.otel_telemetry.enable_metrics {
            init_otel_metrics(config.otel_telemetry.grpc_endpoint).map_err(|e| ReplicatorError {
                kind: ReplicatorErrorKind::Internal,
                reason: "Unable to initialise OTEL metrics for replicator".into(),
                cause: Some(format!("{:?}", e)),
            })?;
        }
    } else {
        tracing::warn!("OTEL will not be initialised for this replicator instance, it may still be used if another module initialises it.")
    }

    // ---------- Channels to communicate between threads. ----------

    // Replicator to Statemap queue
    let (tx_replicator_to_statemap_queue, rx_replicator_to_statemap_queue) = create_channel(config.channel_size);
    // Statemap installer feedback to replicator
    let (tx_installation_feedback_to_replicator, rx_installation_feedback_to_replicator) = create_channel(config.channel_size);
    // Statemap queue service to Statemap installer service - Statemaps to install
    let (tx_statemaps_to_install, rx_statemaps_to_install) = create_channel(config.channel_size);
    // Statemap Installer service feedback to Statemap queue service - post install feedback.
    let (tx_statemaps_install_feedback, rx_statemaps_install_feedback) = create_channel(config.channel_size);

    //  ---------------------------------------------------------------

    // Replicator Service
    let suffix_config = SuffixConfig {
        capacity: config.suffix_capacity,
        prune_start_threshold: config.suffix_prune_threshold,
        min_size_after_prune: config.suffix_minimum_size_on_prune,
    };
    let suffix: Suffix<ReplicatorCandidate> = Suffix::with_config(suffix_config);

    let meter = if config.otel_telemetry.enable_metrics {
        Some(global::meter("cohort_sdk_replicator"))
    } else {
        None
    };

    let replicator = Replicator::new(certifier_message_receiver, suffix, meter.clone());

    let replicator_service_configs = ReplicatorServiceConfig {
        commit_frequency_ms: config.certifier_message_receiver_commit_freq_ms,
        enable_stats: config.enable_stats,
    };
    let replicator_handle = tokio::spawn(replicator_service(
        tx_replicator_to_statemap_queue,
        rx_installation_feedback_to_replicator,
        replicator,
        replicator_service_configs,
    ));

    // Statemap Queue Service
    let queue_config = StatemapQueueServiceConfig {
        enable_stats: config.enable_stats,
        queue_cleanup_frequency_ms: config.statemap_queue_cleanup_freq_ms,
    };

    let statemap_queue_handle = tokio::spawn(statemap_queue_service(
        rx_replicator_to_statemap_queue,
        rx_statemaps_install_feedback,
        tx_statemaps_to_install,
        snapshot_api,
        queue_config,
        config.channel_size,
        meter,
    ));

    // Statemap Installation Service
    let installer_config = StatemapInstallerConfig {
        thread_pool: Some(config.statemap_installer_threadpool as u16),
    };

    let statemap_installer_handle = tokio::spawn(installation_service(
        tx_installation_feedback_to_replicator,
        Arc::clone(&statemap_installer),
        rx_statemaps_to_install,
        tx_statemaps_install_feedback,
        installer_config,
    ));

    try_join!(
        flatten_service_result(replicator_handle),
        flatten_service_result(statemap_queue_handle),
        flatten_service_result(statemap_installer_handle)
    )
}
