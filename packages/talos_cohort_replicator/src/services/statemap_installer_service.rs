// $coverage:ignore-start

use std::{sync::Arc, time::Instant};

use crate::{
    callbacks::ReplicatorInstaller,
    core::{StatemapInstallationStatus, StatemapItem},
    errors::ReplicatorError,
};

use opentelemetry::global;
use time::OffsetDateTime;
use tokio::sync::{mpsc, Semaphore};
use tracing::{debug, error, info};

pub struct StatemapInstallerConfig {
    pub thread_pool: Option<u16>,
}

async fn statemap_install_future(
    installer: Arc<dyn ReplicatorInstaller + Send + Sync>,
    statemap_installation_tx: mpsc::Sender<StatemapInstallationStatus>,
    statemaps: Vec<StatemapItem>,
    version: u64,
) {
    debug!("[Statemap Installer Service] Received statemap batch ={statemaps:?} and version={version:?}");
    let start_installation_time = Instant::now();

    let meter = global::meter("sdk_replicator");
    let c_installed = meter.u64_counter("repl_install_ok").build();
    let c_install_failed = meter.u64_counter("repl_install_err").build();
    let h_install_latency = meter.f64_histogram("repl_install_latency").build();
    let started_at = OffsetDateTime::now_utc().unix_timestamp_nanos();

    info!(
        "[Version>>{version}] before calling the installer callback {:?}ms",
        OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000_i128
    );

    match installer.install(statemaps, version).await {
        Ok(_) => {
            statemap_installation_tx.send(StatemapInstallationStatus::Success(version)).await.unwrap();
            c_installed.add(1, &[]);
            let latency_ms = (OffsetDateTime::now_utc().unix_timestamp_nanos() - started_at) as f64 / 1_000_000_f64;
            h_install_latency.record(latency_ms, &[]);
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

    info!(
        "[Version>>{version}] after calling the installer callback {:?}ms",
        OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000_i128
    );
}

pub async fn installation_service(
    statemap_installer: Arc<dyn ReplicatorInstaller + Send + Sync>,
    mut installation_rx: mpsc::Receiver<(u64, Vec<StatemapItem>)>,
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
        if let Some((ver, statemaps)) = installation_rx.recv().await {
            info!(
                "[Version>>{ver}] received in installer thread at {:?}ms",
                OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000_i128
            );
            let permit = semaphore.acquire_owned().await.unwrap();
            info!(
                "[Version>>{ver}] permit acquired in installer thread at {:?}ms",
                OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000_i128
            );
            let installer = Arc::clone(&statemap_installer);
            let statemap_installation_tx_clone = statemap_installation_tx.clone();
            udc_items_installing_copy.add(1, &[]);
            tokio::spawn(async move {
                statemap_install_future(installer, statemap_installation_tx_clone, statemaps, ver).await;
                drop(permit);
                udc_items_installing_copy.add(-1, &[]);
            });
        }
    }
}
// $coverage:ignore-end
