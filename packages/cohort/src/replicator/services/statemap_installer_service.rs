// $coverage:ignore-start

use std::{sync::Arc, time::Instant};

use crate::replicator::core::{ReplicatorChannel, ReplicatorInstallStatus, ReplicatorInstaller, StatemapInstallationStatus, StatemapItem};

use log::{debug, error};
use tokio::sync::{mpsc, Semaphore};

pub struct StatemapInstallerConfig {
    pub parallel_thread_count: Option<u16>,
}

pub async fn installation_service(
    replicator_tx: mpsc::Sender<ReplicatorChannel>,
    statemap_installer: Arc<dyn ReplicatorInstaller + Send + Sync>,
    mut installation_rx: mpsc::Receiver<(u64, Vec<StatemapItem>)>,
    statemap_installation_tx: mpsc::Sender<StatemapInstallationStatus>,
    config: StatemapInstallerConfig,
) -> Result<(), String> {
    // TODO: Pass the number of permits over an environment variable?
    let permit_count = config.parallel_thread_count.unwrap_or(50) as usize;
    let semaphore = Arc::new(Semaphore::new(permit_count));

    loop {
        let available_permits = semaphore.available_permits();

        if available_permits > 0 {
            if let Some((ver, statemaps)) = installation_rx.recv().await {
                let replicator_tx_clone = replicator_tx.clone();
                let statemap_installation_tx_clone = statemap_installation_tx.clone();
                let installer = Arc::clone(&statemap_installer);

                let permit = semaphore.clone().acquire_owned().await.unwrap();
                tokio::spawn(async move {
                    debug!("[Statemap Installer Service] Received statemap batch ={statemaps:?} and version={ver:?}");
                    let start_installation_time = Instant::now();

                    match installer.install(statemaps, Some(ver)).await {
                        Ok(status) => {
                            // let end_installation_time = start_installation_time.elapsed();
                            // error!("[installation_service] Installed successfully version={ver} in {end_installation_time:?}");
                            replicator_tx_clone.send(ReplicatorChannel::InstallationSuccess(vec![ver])).await.unwrap();
                            match status {
                                ReplicatorInstallStatus::Success => {
                                    statemap_installation_tx_clone.send(StatemapInstallationStatus::Success(ver)).await.unwrap();
                                }
                                ReplicatorInstallStatus::Gaveup(_) => {
                                    statemap_installation_tx_clone.send(StatemapInstallationStatus::GaveUp(ver)).await.unwrap();
                                }
                            }

                            drop(permit);
                        }

                        Err(err) => {
                            error!(
                                "Installed failed for version={ver:?} with time={:?} error={err:?}",
                                start_installation_time.elapsed()
                            );
                            replicator_tx_clone
                                .send(ReplicatorChannel::InstallationFailure("Crash and Burn!!!".to_string()))
                                .await
                                .unwrap();
                            statemap_installation_tx_clone
                                .send(StatemapInstallationStatus::Error(
                                    ver,
                                    "🔥🔥🔥 Crash and burn the statemap installer queue service".to_string(),
                                ))
                                .await
                                .unwrap();
                            drop(permit);
                        }
                    };
                });
            };
        };
    }
}
// $coverage:ignore-end
