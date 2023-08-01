// $coverage:ignore-start

use std::{sync::Arc, time::Instant};

use crate::core::{ReplicatorChannel, ReplicatorInstallStatus, ReplicatorInstaller, StatemapInstallationStatus, StatemapItem};

use log::{debug, error};
use tokio::sync::{mpsc, Semaphore};

pub struct StatemapInstallerConfig {
    pub thread_pool: Option<u16>,
}

pub async fn installation_service(
    replicator_tx: mpsc::Sender<ReplicatorChannel>,
    statemap_installer: Arc<dyn ReplicatorInstaller + Send + Sync>,
    mut installation_rx: mpsc::Receiver<(u64, Vec<StatemapItem>)>,
    statemap_installation_tx: mpsc::Sender<StatemapInstallationStatus>,
    config: StatemapInstallerConfig,
) -> Result<(), String> {
    let permit_count = config.thread_pool.unwrap_or(50) as usize;
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

                    match installer.install(statemaps, ver).await {
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
                                .send(ReplicatorChannel::InstallationFailure(format!(
                                    "Crash and Burn!!! Installed failed for version={ver:?} error={err:?}"
                                )))
                                .await
                                .unwrap();
                            statemap_installation_tx_clone
                                .send(StatemapInstallationStatus::Error(
                                    ver,
                                    format!("🔥🔥🔥 Crash and burn the statemap installer queue service Installed failed for version={ver:?} error={err:?}"),
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
