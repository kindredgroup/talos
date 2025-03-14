// $coverage:ignore-start

use std::{sync::Arc, time::Instant};

use crate::{
    callbacks::ReplicatorInstaller,
    core::{ReplicatorChannel, StatemapInstallationStatus, StatemapItem},
    errors::ReplicatorError,
};

use tracing::{debug, error};
use tokio::sync::{mpsc, Semaphore};

pub struct StatemapInstallerConfig {
    pub thread_pool: Option<u16>,
}

async fn statemap_install_future(
    installer: Arc<dyn ReplicatorInstaller + Send + Sync>,
    replicator_tx: mpsc::Sender<ReplicatorChannel>,
    statemap_installation_tx: mpsc::Sender<StatemapInstallationStatus>,
    semaphore: Arc<Semaphore>,
    statemaps: Vec<StatemapItem>,
    version: u64,
) {
    debug!("[Statemap Installer Service] Received statemap batch ={statemaps:?} and version={version:?}");
    let start_installation_time = Instant::now();

    let permit = semaphore.clone().acquire_owned().await.unwrap();

    match installer.install(statemaps, version).await {
        Ok(_) => {
            replicator_tx.send(ReplicatorChannel::InstallationSuccess(vec![version])).await.unwrap();
            statemap_installation_tx.send(StatemapInstallationStatus::Success(version)).await.unwrap();
        }

        Err(err) => {
            error!(
                "Installed failed for version={version:?} with time={:?} error={err:?}",
                start_installation_time.elapsed()
            );
            replicator_tx
                .send(ReplicatorChannel::InstallationFailure(format!(
                    "Failed to install version={version:?} with error={err:?}"
                )))
                .await
                .unwrap();
            statemap_installation_tx
                .send(StatemapInstallationStatus::Error(
                    version,
                    format!("🔥🔥🔥 The statemap installer queue service install failed for version={version:?} error={err:?}"),
                ))
                .await
                .unwrap();
        }
    };
    drop(permit);
}

pub async fn installation_service(
    replicator_tx: mpsc::Sender<ReplicatorChannel>,
    statemap_installer: Arc<dyn ReplicatorInstaller + Send + Sync>,
    mut installation_rx: mpsc::Receiver<(u64, Vec<StatemapItem>)>,
    statemap_installation_tx: mpsc::Sender<StatemapInstallationStatus>,
    config: StatemapInstallerConfig,
) -> Result<(), ReplicatorError> {
    let permit_count = config.thread_pool.unwrap_or(50) as usize;
    let semaphore = Arc::new(Semaphore::new(permit_count));

    loop {
        let available_permits = semaphore.available_permits();

        if available_permits > 0 {
            if let Some((ver, statemaps)) = installation_rx.recv().await {
                let replicator_tx_clone = replicator_tx.clone();
                let statemap_installation_tx_clone = statemap_installation_tx.clone();
                let installer = Arc::clone(&statemap_installer);

                //  Spawn new task to install the statemap
                tokio::spawn(statemap_install_future(
                    installer,
                    replicator_tx_clone,
                    statemap_installation_tx_clone,
                    semaphore.clone(),
                    statemaps,
                    ver,
                ));
            };
        };
    }
}
// $coverage:ignore-end
