// $coverage:ignore-start

use crate::{
    replicator::{
        core::{ReplicatorChannel, ReplicatorInstaller, StatemapItem},
        statistics::core::ReplicatorStatisticsChannelMessage,
    },
    replicator_update_stats_time,
};

use log::{debug, info};
use tokio::sync::mpsc;

pub async fn installer_service<T>(
    mut statemaps_rx: mpsc::Receiver<(Vec<StatemapItem>, Option<u64>)>,
    replicator_tx: mpsc::Sender<ReplicatorChannel>,
    mut statemap_installer: T,
    statistics_tx: Option<mpsc::Sender<ReplicatorStatisticsChannelMessage>>,
) -> Result<(), String>
where
    T: ReplicatorInstaller,
{
    info!("Starting Installer Service.... ");

    let capture_stats = statistics_tx.is_some();

    loop {
        if let Some((statemap_batch, version_option)) = statemaps_rx.recv().await {
            debug!("[Statemap Installer Service] Received statemap batch ={statemap_batch:?} and version_option={version_option:?}");
            let version = version_option.unwrap();

            replicator_update_stats_time!(statistics_tx, StatemapInstallationTime, version, {
                match statemap_installer.install(statemap_batch, version_option).await {
                    Ok(val) => {
                        if val {
                            replicator_tx
                                .send(ReplicatorChannel::InstallationSuccess(vec![version_option.unwrap()]))
                                .await
                                .unwrap();
                        }

                        if capture_stats {
                            let _ = statistics_tx
                                .as_ref()
                                .unwrap()
                                .send(ReplicatorStatisticsChannelMessage::StatemapInstallationFlag(version_option.unwrap(), val))
                                .await;
                        };
                    }
                    Err(err) => {
                        if capture_stats {
                            let _ = statistics_tx
                                .as_ref()
                                .unwrap()
                                .send(ReplicatorStatisticsChannelMessage::StatemapInstallationFlag(version_option.unwrap(), false))
                                .await;
                        };

                        return Err(err.to_string());
                    }
                };
            });
        }
    }
}
// $coverage:ignore-end
