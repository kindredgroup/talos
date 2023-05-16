// $coverage:ignore-start

use crate::replicator::core::{ReplicatorChannel, ReplicatorInstaller, StatemapItem};

use log::{debug, info};
use tokio::sync::mpsc;

pub async fn installer_service<T>(
    mut statemaps_rx: mpsc::Receiver<(Vec<StatemapItem>, Option<u64>)>,
    replicator_tx: mpsc::Sender<ReplicatorChannel>,
    mut statemap_installer: T,
) -> Result<(), String>
where
    T: ReplicatorInstaller,
{
    info!("Starting Installer Service.... ");

    loop {
        if let Some((statemap_batch, version_option)) = statemaps_rx.recv().await {
            debug!("[Statemap Installer Service] Received statemap batch ={statemap_batch:?} and version_option={version_option:?}");
            match statemap_installer.install(statemap_batch, version_option).await {
                Ok(true) => {
                    replicator_tx
                        .send(ReplicatorChannel::InstallationSuccess(vec![version_option.unwrap()]))
                        .await
                        .unwrap();
                }
                Ok(false) => {
                    // Do nothing if result is false.
                }
                Err(err) => return Err(err.to_string()),
            }
        }
    }
}
// $coverage:ignore-end
