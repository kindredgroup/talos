// $coverage:ignore-start

use std::time::Instant;

use crate::replicator::{
    core::{ReplicatorChannel, ReplicatorInstaller, StatemapItem},
    statistics::core::ReplicatorStatisticsChannelMessage,
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
            let stats_suffix_insert_time = Instant::now();

            match statemap_installer.install(statemap_batch, version_option).await {
                Ok(true) => {
                    replicator_tx
                        .send(ReplicatorChannel::InstallationSuccess(vec![version_option.unwrap()]))
                        .await
                        .unwrap();

                    let stats_suffix_insert_time_elapse = stats_suffix_insert_time.elapsed();
                    if capture_stats {
                        let _ = statistics_tx
                            .as_ref()
                            .unwrap()
                            .send(ReplicatorStatisticsChannelMessage::StatemapInstallationTime(
                                version_option.unwrap(),
                                stats_suffix_insert_time_elapse.as_nanos(),
                                true,
                            ))
                            .await;
                    };
                }
                Ok(false) => {
                    let stats_suffix_insert_time_elapse = stats_suffix_insert_time.elapsed();
                    if capture_stats {
                        let _ = statistics_tx
                            .as_ref()
                            .unwrap()
                            .send(ReplicatorStatisticsChannelMessage::StatemapInstallationTime(
                                version_option.unwrap(),
                                stats_suffix_insert_time_elapse.as_nanos(),
                                false,
                            ))
                            .await;
                    };
                }
                Err(err) => {
                    let stats_suffix_insert_time_elapse = stats_suffix_insert_time.elapsed();
                    if capture_stats {
                        let _ = statistics_tx
                            .as_ref()
                            .unwrap()
                            .send(ReplicatorStatisticsChannelMessage::StatemapInstallationTime(
                                version_option.unwrap(),
                                stats_suffix_insert_time_elapse.as_nanos(),
                                false,
                            ))
                            .await;
                    };

                    return Err(err.to_string());
                }
            }
        }
    }
}
// $coverage:ignore-end
