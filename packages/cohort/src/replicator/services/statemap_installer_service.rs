// $coverage:ignore-start

use crate::replicator::core::{ReplicatorChannel, ReplicatorInstaller, StatemapItem};

use log::{debug, info};
use tokio::sync::mpsc;

pub async fn installer_service<T>(
    mut statemaps_rx: mpsc::Receiver<Vec<(u64, Vec<StatemapItem>)>>,
    replicator_tx: mpsc::Sender<ReplicatorChannel>,
    mut statemap_installer: T,
) -> Result<(), String>
where
    T: ReplicatorInstaller,
{
    info!("Starting Installer Service.... ");

    loop {
        if let Some(batch) = statemaps_rx.recv().await {
            for (ver, statemap_batch) in batch {
                debug!("[Statemap Installer Service] Received statemap batch ={statemap_batch:?} and version={ver:?}");
                match statemap_installer.install(statemap_batch, Some(ver)).await {
                    Ok(true) => {
                        replicator_tx.send(ReplicatorChannel::InstallationSuccess(vec![ver])).await.unwrap();
                    }
                    Ok(false) => {
                        // Do nothing if result is false.
                    }
                    Err(err) => return Err(err.to_string()),
                }
            }
        }
    }
}
// $coverage:ignore-end
