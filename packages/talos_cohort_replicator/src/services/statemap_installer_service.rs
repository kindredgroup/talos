// $coverage:ignore-start

use std::{sync::Arc, time::Instant};

use crate::{
    callbacks::ReplicatorInstaller,
    core::{StatemapInstallationStatus, StatemapItem},
    errors::ReplicatorError,
    events::{ReplicatorCandidateEvent, ReplicatorCandidateEventTimingsTrait, StatemapEvents},
};

use opentelemetry::global;
use time::OffsetDateTime;
use tokio::sync::{mpsc, Semaphore};
use tracing::{debug, error};

pub struct StatemapInstallerConfig {
    pub thread_pool: Option<u16>,
}

pub async fn statemap_install_future(
    installer: Arc<dyn ReplicatorInstaller + Send + Sync>,
    statemap_installation_tx: mpsc::Sender<StatemapInstallationStatus>,
    statemaps: Vec<StatemapItem>,
    statemap_events: &mut StatemapEvents,
    version: u64,
) {
    debug!("[Statemap Installer Service] Received statemap batch ={statemaps:?} and version={version:?}");
    let start_installation_time = Instant::now();

    // Otel meter and metrics
    let meter = global::meter("sdk_replicator");
    let c_installed = meter.u64_counter("repl_install_ok").build();
    let c_install_failed = meter.u64_counter("repl_install_err").build();
    let h_install_latency = meter.f64_histogram("repl_install_latency").build();
    let h_candidate_to_install_latency = meter.f64_histogram("repl_candidate_to_install_latency").build();
    let h_decision_to_install_latency = meter.f64_histogram("repl_decision_to_install_latency").build();

    let started_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
    statemap_events.record_event_timestamp(ReplicatorCandidateEvent::InstallerStatemapInstallionBegin, started_at);

    match installer.install(statemaps, version).await {
        Ok(_) => {
            statemap_installation_tx.send(StatemapInstallationStatus::Success(version)).await.unwrap();
            c_installed.add(1, &[]);
            let current_time_ns = OffsetDateTime::now_utc().unix_timestamp_nanos();

            // Record the otel metric for installation latency
            let latency_ms = (current_time_ns - started_at) as f64 / 1_000_000_f64;
            h_install_latency.record(latency_ms, &[]);

            statemap_events.record_event_timestamp(ReplicatorCandidateEvent::InstallerStatemapInstallionComplete, current_time_ns);
            // Record latency from the time the candidate was received to when the statemaps were installed
            if let Some(candidate_received_at_ns) = statemap_events.get_event_timestamp(ReplicatorCandidateEvent::ReplicatorCandidateReceived) {
                h_candidate_to_install_latency.record((current_time_ns - candidate_received_at_ns) as f64 / 1_000_000_f64, &[])
            }
            // Record latency from the time the decision was received to when the statemaps were installed
            if let Some(decision_received_at_ns) = statemap_events.get_event_timestamp(ReplicatorCandidateEvent::ReplicatorDecisionReceived) {
                h_decision_to_install_latency.record((current_time_ns - decision_received_at_ns) as f64 / 1_000_000_f64, &[])
            }
        }

        Err(err) => {
            c_install_failed.add(1, &[]);
            error!(
                "Installed failed for version={version:?} with time={:?} error={err:?}",
                start_installation_time.elapsed()
            );
            statemap_installation_tx
                .send(StatemapInstallationStatus::Error(
                    version,
                    format!("🔥🔥🔥 The statemap installer queue service install failed for version={version:?} error={err:?}"),
                ))
                .await
                .unwrap();
        }
    };
    // drop(permit);
}

pub async fn installation_service(
    statemap_installer: Arc<dyn ReplicatorInstaller + Send + Sync>,
    mut installation_rx: mpsc::Receiver<(u64, Vec<StatemapItem>, StatemapEvents)>,
    statemap_installation_tx: mpsc::Sender<StatemapInstallationStatus>,
    config: StatemapInstallerConfig,
) -> Result<(), ReplicatorError> {
    let permit_count = config.thread_pool.unwrap_or(50) as usize;
    let semaphore = Arc::new(Semaphore::new(permit_count));
    let udc_items_installing = global::meter("sdk_replicator")
        .i64_up_down_counter("repl_items_installing")
        .with_unit("items")
        .build();

    loop {
        let semaphore = semaphore.clone();
        let udc_items_installing_copy = udc_items_installing.clone();
        if let Some((ver, statemaps, mut event_timings)) = installation_rx.recv().await {
            // Capture the time when the statemaps arrived at the installer thread
            event_timings.record_event_timestamp(
                ReplicatorCandidateEvent::InstallerStatemapReceived,
                OffsetDateTime::now_utc().unix_timestamp_nanos(),
            );
            let permit = semaphore.acquire_owned().await.unwrap();
            let installer = Arc::clone(&statemap_installer);
            let statemap_installation_tx_clone = statemap_installation_tx.clone();
            udc_items_installing_copy.add(1, &[]);

            tokio::spawn({
                async move {
                    statemap_install_future(installer, statemap_installation_tx_clone, statemaps, &mut event_timings, ver).await;
                    drop(permit);
                    udc_items_installing_copy.add(-1, &[]);
                }
            });
        }
    }
}
// $coverage:ignore-end
