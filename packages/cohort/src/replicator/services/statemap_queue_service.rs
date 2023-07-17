// $coverage:ignore-start

use std::time::Duration;

use futures::Future;
use log::{error, info};
use rayon::prelude::ParallelIterator;
use time::OffsetDateTime;
use tokio::sync::mpsc;

use crate::replicator::{
    core::{StatemapInstallState, StatemapInstallationStatus, StatemapInstallerHashmap, StatemapItem},
    utils::installer_utils::StatemapInstallerQueue,
};

pub async fn statemap_queue_service(
    mut statemaps_rx: mpsc::Receiver<Vec<StatemapItem>>,
    mut statemap_installation_rx: mpsc::Receiver<StatemapInstallationStatus>,
    installation_tx: mpsc::Sender<(u64, Vec<StatemapItem>)>,
    // Get snapshot callback fn
    get_snapshot_fn: impl Future<Output = Result<u64, String>>,
) -> Result<(), String> {
    info!("Starting Installer Queue Service.... ");
    // let last_installed = 0_u64;
    // let mut statemap_queue = IndexMap::<u64, StatemapInstallerHashmap, RandomState>::default();
    let mut interval = tokio::time::interval(Duration::from_millis(10_000));

    let mut installation_success_count = 0;
    let mut installation_gaveup = 0;
    let mut send_for_install_count = 0;
    let mut first_install_start: i128 = 0; //
    let mut last_install_end: i128 = 0; //  = OffsetDateTime::now_utc().unix_timestamp_nanos();

    let mut statemap_installer_queue = StatemapInstallerQueue::default();

    //Gets snapshot initial version from db.
    statemap_installer_queue.update_snapshot(get_snapshot_fn.await.unwrap_or(0));

    let mut last_item_send_for_install = 0;

    loop {
        tokio::select! {
            statemap_batch_option = statemaps_rx.recv() => {

                if let Some(statemaps) = statemap_batch_option {

                    let ver = statemaps.first().unwrap().version;
                    // Inserts the statemaps to the map

                    let safepoint = if let Some(first_statemap) = statemaps.first() {
                        first_statemap.safepoint
                    } else {
                        None
                    };
                    statemap_installer_queue.insert_queue_item(&ver, StatemapInstallerHashmap { statemaps, version: ver, safepoint, state: StatemapInstallState::Awaiting });

                    // Gets the statemaps to send for installation.
                    let  items_to_install: Vec<u64> = statemap_installer_queue.get_versions_to_install();

                    // Sends for installation.
                    for key in items_to_install {
                        // Send for installation
                        // warn!("Sending... {key}");
                        if send_for_install_count == 0 {
                            first_install_start = OffsetDateTime::now_utc().unix_timestamp_nanos();
                        }
                        send_for_install_count += 1;
                        installation_tx.send((key, statemap_installer_queue.queue.get(&key).unwrap().statemaps.clone())).await.unwrap();

                        last_item_send_for_install = key;

                        // Update the status flag
                        statemap_installer_queue.update_queue_item_state(&key, StatemapInstallState::Inflight);
                    }
                }
            }
            Some(install_result) = statemap_installation_rx.recv() => {
                match install_result {
                    StatemapInstallationStatus::Success(key) => {
                        // let start_time_success = Instant::now();

                        // installed successfully and will remove the item
                        // statemap_queue.shift_remove(&key);
                        statemap_installer_queue.update_queue_item_state(&key, StatemapInstallState::Installed);

                        // let index = statemap_queue.get_index_of(&key).unwrap();

                        if let Some(last_contiguous_install_item) = statemap_installer_queue.queue.iter().take_while(|item| item.1.state == StatemapInstallState::Installed).last(){
                            statemap_installer_queue.update_snapshot(last_contiguous_install_item.1.version) ;
                        };


                        installation_success_count += 1;
                        last_install_end = OffsetDateTime::now_utc().unix_timestamp_nanos();
                        // error!("Installed successfully version={key} and total_installs={install_count}");
                        // let end_time_success = start_time_success.elapsed();
                        // error!("(Statemap successfully installed) for version={key} in {end_time_success:?}");
                    },
                    StatemapInstallationStatus::GaveUp(_) => {
                        installation_gaveup += 1;
                        last_install_end = OffsetDateTime::now_utc().unix_timestamp_nanos();
                    },
                    StatemapInstallationStatus::Error(ver, error) => {
                        error!("Failed to install version={ver} due to error={error:?}");
                        let items_in_flight: Vec<&StatemapInstallerHashmap> = statemap_installer_queue.queue
                            .par_values()
                            // Picking waiting items
                            .filter_map(|v| {
                                if v.state == StatemapInstallState::Inflight || v.state == StatemapInstallState::Installed {
                                    Some(v)
                                } else {
                                    None
                                }
                        }).collect();
                        panic!("[Panic Panic Panic] panic for ver={ver} with error={error} \n\n\n Items still in statemap \n\n\n {items_in_flight:#?}");
                    },
                }
            }
            _ = interval.tick() => {

                let duration_sec = Duration::from_nanos((last_install_end - first_install_start) as u64).as_secs_f32();
                let tps = installation_success_count as f32 / duration_sec;

                let awaiting_count = statemap_installer_queue.filter_items_by_state(StatemapInstallState::Awaiting).count();
                let inflight_count = statemap_installer_queue.filter_items_by_state(StatemapInstallState::Inflight).count();
                error!("Currently Statemap installation tps={tps:.3}");
                error!("
                Statemap Installer Queue Stats:
                      tps             : {tps:.3}
                      counts          :
                                      | success={installation_success_count}
                                      | gaveup={installation_gaveup}
                                      | awaiting_installs={awaiting_count}
                                      | inflight_count={inflight_count}
                                      | installation_gaveup={installation_gaveup}
                      current snapshot: {}
                      last vers send to install : {last_item_send_for_install}
                                      \n ", statemap_installer_queue.snapshot_version);

                statemap_installer_queue.remove_installed();
            }

        }
    }
}
// $coverage:ignore-end
