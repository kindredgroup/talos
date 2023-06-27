// $coverage:ignore-start

use crate::replicator::core::{ReplicatorChannel, ReplicatorInstaller, ReplicatorInstallerMetric, ReplicatorInstallerMetricBuilder, StatemapItem};

use log::{debug, info};
use time::OffsetDateTime;
use tokio::sync::mpsc;

pub async fn installer_service<T>(
    mut statemaps_rx: mpsc::Receiver<(Vec<StatemapItem>, Option<u64>, i128, i128)>,
    replicator_tx: mpsc::Sender<ReplicatorChannel>,
    mut statemap_installer: T,
) -> Result<(), String>
where
    T: ReplicatorInstaller,
{
    info!("Starting Installer Service.... ");

    loop {
        let channel_receive_time = OffsetDateTime::now_utc().unix_timestamp_nanos();
        if let Some((statemap_batch, version_option, replicator_start_time, channel_send_time)) = statemaps_rx.recv().await {
            debug!("[Statemap Installer Service] Received statemap batch ={statemap_batch:?} and version_option={version_option:?}");
            let install_start_time = OffsetDateTime::now_utc().unix_timestamp_nanos();
            match statemap_installer.install(statemap_batch, version_option).await {
                Ok(true) => {
                    let install_end_time = OffsetDateTime::now_utc().unix_timestamp_nanos();
                    let replicator_installer_metric = ReplicatorInstallerMetricBuilder::new()
                        .add_replicator_start_time(replicator_start_time)
                        .add_channel_send_time(channel_send_time)
                        .add_channel_receive_time(channel_receive_time)
                        .add_installation_start_time(install_start_time)
                        .add_installation_end_time(install_end_time);

                    replicator_tx
                        .send(ReplicatorChannel::InstallationSuccess(
                            vec![version_option.unwrap()],
                            replicator_installer_metric,
                        ))
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
