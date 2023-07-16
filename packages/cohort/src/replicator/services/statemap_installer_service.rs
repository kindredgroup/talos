// $coverage:ignore-start

use std::{
    hash::BuildHasherDefault,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    model::requests::TransferRequest,
    replicator::core::{ReplicatorChannel, ReplicatorInstallStatus, ReplicatorInstaller, StatemapItem},
};

use ahash::{HashSet, HashSetExt, RandomState};
use indexmap::{IndexMap, IndexSet};
use log::{debug, error, info, warn};
use rayon::prelude::*;
use time::OffsetDateTime;
use tokio::{
    signal,
    sync::{mpsc, Semaphore},
};
use tokio_util::sync::PollSemaphore;

#[derive(Debug, Clone, PartialEq)]
pub enum StatemapInstallState {
    Awaiting,
    Inflight,
    Installed,
}
#[derive(Debug, Clone)]
pub struct StatemapInstallerHashmap {
    pub statemaps: Vec<StatemapItem>,
    pub version: u64,
    pub safepoint: Option<u64>,
    pub status: StatemapInstallState,
}

pub async fn installer_service<T>(
    mut statemaps_rx: mpsc::Receiver<Vec<(u64, Vec<StatemapItem>)>>,
    replicator_tx: mpsc::Sender<ReplicatorChannel>,
    statemap_installer: T,
) -> Result<(), String>
where
    T: ReplicatorInstaller,
{
    info!("Starting Installer Service.... ");

    // loop {
    //     if let Some(batch) = statemaps_rx.recv().await {
    //         for (ver, statemap_batch) in batch {
    //             debug!("[Statemap Installer Service] Received statemap batch ={statemap_batch:?} and version={ver:?}");
    //             match statemap_installer.install(statemap_batch, Some(ver)).await {
    //                 Ok(true) => {
    //                     replicator_tx.send(ReplicatorChannel::InstallationSuccess(vec![ver])).await.unwrap();
    //                 }
    //                 Ok(false) => {
    //                     // Do nothing if result is false.
    //                 }
    //                 Err(err) => return Err(err.to_string()),
    //             }
    //         }
    //     }
    // }

    Ok(())
}

#[derive(Debug)]
pub enum StatemapInstallationStatus {
    Success(u64),
    GaveUp(u64),
    Error(u64, String),
}

pub async fn installer_queue_service(
    mut statemaps_rx: mpsc::Receiver<Vec<StatemapItem>>,
    mut statemap_installation_rx: mpsc::Receiver<StatemapInstallationStatus>,
    installation_tx: mpsc::Sender<(u64, Vec<StatemapItem>)>,
) -> Result<(), String> {
    info!("Starting Installer Queue Service.... ");
    // let last_installed = 0_u64;
    let mut statemap_queue = IndexMap::<u64, StatemapInstallerHashmap, RandomState>::default();
    let mut interval = tokio::time::interval(Duration::from_millis(10_000));

    let mut installation_success_count = 0;
    let mut installation_gaveup = 0;
    let mut send_for_install_count = 0;
    let mut first_install_start: i128 = 0; //
    let mut last_install_end: i128 = 0; //  = OffsetDateTime::now_utc().unix_timestamp_nanos();

    // TODO: Get snapshot initial version from db.
    let mut snapshot_version = 0;
    let mut last_item_send_for_install = 0;

    let mut prev_last_item_send_for_install = 0;
    let mut hang_state_try = 5;
    loop {
        let enable_extra_logging = hang_state_try == 0;
        tokio::select! {
            statemap_batch_option = statemaps_rx.recv() => {

                if let Some(statemaps) = statemap_batch_option {

                    let ver = statemaps.first().unwrap().version;
                    // let last_version = batch.last().unwrap();

                    let start_time_insert = Instant::now();
                    // Inserts the statemaps to the map
                    // for (ver, statemap_batch) in batch {

                    let safepoint = if let Some(first_statemap) = statemaps.first() {
                        first_statemap.safepoint
                    } else {
                        None
                    };
                    statemap_queue.insert(ver, StatemapInstallerHashmap { statemaps, version: ver, safepoint, status: StatemapInstallState::Awaiting });

                    // }
                    let insert_elapsed = start_time_insert.elapsed();

                    // Gets the statemaps to send for installation.
                    let mut items_to_install: Vec<u64> = vec![];

                    // let last_installed_index = statemap_queue.get_index_of(&last_installed).unwrap_or(0);
                    // let awaiting_range = last_installed_index..;
                    let start_time_create_install_items = Instant::now();


                    let items = statemap_queue.get_range(..);

                    // let in_flight_accounts:HashSet<&String> = statemap_queue.values().filter(|v| v.status == StatemapInstallState::Inflight).fold( HashSet::new(),|mut acc,v| {
                    //     if let Some(first_statemap) = v.statemaps.first() {
                    //         first_statemap.lookup_keys.iter().for_each(|k| {
                    //             acc.insert(&k);
                    //         })
                    //     }
                    //     acc
                    // });


                    if let Some(statemap_items) = items {

                        items_to_install = statemap_items
                        .values()
                        // Picking waiting items
                        .filter(|v| v.status == StatemapInstallState::Awaiting)
                        //  TODO: use ids instead of direct check on TransferRequest
                        //  Observed: Using filter instead of take_while, causes performance deterioration.
                        .take_while(|v| {
                            // If no safepoint, this could be a abort item and is safe to install as statemap will be empty.
                            let Some(safepoint) = v.safepoint else {
                                return true;
                            };

                            if snapshot_version >= safepoint {
                                return true;
                            };

                            // if !res {
                            //     let first_sm_of_picked = v.statemaps.first().unwrap();
                            //     let inflight_clashes:HashSet<(u64,&String)> = statemap_queue.values().filter(|v| v.status == StatemapInstallState::Inflight).fold( HashSet::new(),|mut acc,inflight_v| {
                            //         if let Some(first_statemap) = inflight_v.statemaps.first() {
                            //             first_statemap.lookup_keys.iter().for_each(|k| {
                            //                 if first_sm_of_picked.lookup_keys.contains(k) {
                            //                     acc.insert((inflight_v.version,&k));
                            //                 }
                            //             })
                            //         }
                            //         acc
                            //     });
                            //     error!("[items_to_install] Not picking {} as found writeset in inflight {inflight_clashes:?} ", v.version);

                            // }
                            // error!("Version={} not picked as Snapshot version {snapshot_version} is less than the safepoint version ={safepoint}", v.version);
                            false
                        })
                        // filter out the ones that can't be serialized
                        .filter_map(|v| {
                            // If no safepoint, this could be a abort item and is safe to install as statemap will be empty.
                            let Some(safepoint) = v.safepoint else {
                                return Some(v.version);
                            };

                            // If there is no version matching the safepoint, then it is safe to install
                            let Some(safepoint_pointing_item) = statemap_queue.get(&safepoint) else {
                                return Some(v.version);
                            };
                            if safepoint_pointing_item.status == StatemapInstallState::Installed {
                                return Some(v.version);
                            };
                            // error!("[items_to_install] Not picking {} as safepoint={safepoint} criteria failed against={:?}", v.version, statemap_queue.get(&safepoint));

                            None
                        })
                        // take the remaining we can install
                        .collect::<Vec<u64>>();
                    }

                    let end_time_install_items = start_time_create_install_items.elapsed();


                    let start_time_send = Instant::now();
                    let install_len = items_to_install.len();

                    // Sends for installation.
                    for key in items_to_install {
                        // Send for installation
                        // warn!("Sending... {key}");
                        if send_for_install_count == 0 {
                            first_install_start = OffsetDateTime::now_utc().unix_timestamp_nanos();
                        }
                        send_for_install_count += 1;
                        installation_tx.send((key, statemap_queue.get(&key).unwrap().statemaps.clone())).await.unwrap();

                        last_item_send_for_install = key;

                        // Update the status flag
                        let state_item = statemap_queue.get_mut(&key).unwrap();
                        state_item.status = StatemapInstallState::Inflight;
                    }
                    let end_time_send = start_time_send.elapsed();

                    // error!("Time taken to create versions from={first_version} to={last_version} in  {insert_elapsed:?}. Created batch of {install_len} in {end_time_install_items:?} and send with update items to inflight in {end_time_send:?}");
                }
            }
            Some(install_result) = statemap_installation_rx.recv() => {
                match install_result {
                    StatemapInstallationStatus::Success(key) => {
                        let start_time_success = Instant::now();

                        // installed successfully and will remove the item
                        // statemap_queue.shift_remove(&key);
                        let state_item = statemap_queue.get_mut(&key).unwrap();
                        state_item.status = StatemapInstallState::Installed;

                        // let index = statemap_queue.get_index_of(&key).unwrap();

                        if let Some(last_contiguous_install_item) = statemap_queue.iter().take_while(|item| item.1.status == StatemapInstallState::Installed).last(){
                            snapshot_version = last_contiguous_install_item.1.version;
                        };

                        // if all_prior_installed {
                        //     snapshot_version = key;
                        // }

                        installation_success_count += 1;
                        last_install_end = OffsetDateTime::now_utc().unix_timestamp_nanos();
                        // error!("Installed successfully version={key} and total_installs={install_count}");
                        let end_time_success = start_time_success.elapsed();
                        // error!("(Statemap successfully installed) for version={key} in {end_time_success:?}");
                    },
                    StatemapInstallationStatus::GaveUp(_) => {
                        installation_gaveup += 1;
                        last_install_end = OffsetDateTime::now_utc().unix_timestamp_nanos();
                    },
                    StatemapInstallationStatus::Error(ver, error) => {
                        error!("Failed to install version={ver} due to error={error:?}");
                        let items_in_flight: Vec<&StatemapInstallerHashmap> = statemap_queue
                            .par_values()
                            // Picking waiting items
                            .filter_map(|v| {
                                if v.status == StatemapInstallState::Inflight || v.status == StatemapInstallState::Installed {
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

                // let installed_keys = statemap_queue.par_values().filter_map(|v| {if v.status == StatemapInstallState::Installed {
                //     Some(v.version)
                // } else {
                //     None
                // }})
                // .collect::<Vec<u64>>();

                let duration_sec = Duration::from_nanos((last_install_end - first_install_start) as u64).as_secs_f32();
                let tps = installation_success_count as f32 / duration_sec;

                let awaiting_count = statemap_queue.values().filter(|v| v.status == StatemapInstallState::Awaiting).count();
                if enable_extra_logging {

                    let awaiting_install_versions: Vec<(u64, Option<u64>)> = statemap_queue.values().filter_map(|v| { if v.status == StatemapInstallState::Awaiting {
                            Some((v.version, v.safepoint))
                        } else {
                            None
                        }}).collect();

                        error!("Awaiting (versions, safepoint) tuple to install .... \n {awaiting_install_versions:#?}");

                }

                if prev_last_item_send_for_install == last_item_send_for_install {
                    hang_state_try -= 1;
                }  else {
                    hang_state_try = 5;
                }

                let inflight_count = statemap_queue.values().filter(|v| v.status == StatemapInstallState::Inflight).count();
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
                      current snapshot: {snapshot_version}
                      last vers send to install : {last_item_send_for_install}
                                      \n ");
                                      //   awaiting_install_versions
                                      //   {awaiting_install_versions:?}

                if snapshot_version > 0 {

                    let index = statemap_queue.get_index_of(&snapshot_version).unwrap();

                    statemap_queue.drain(..index);
                }

                prev_last_item_send_for_install = last_item_send_for_install;

                // installed_keys.iter().for_each(|key| {
                //   statemap_queue.shift_remove(key);
                // })
            }

        }
    }
}

pub async fn installation_service(
    replicator_tx: mpsc::Sender<ReplicatorChannel>,
    statemap_installer: Arc<dyn ReplicatorInstaller + Send + Sync>,
    mut installation_rx: mpsc::Receiver<(u64, Vec<StatemapItem>)>,
    statemap_installation_tx: mpsc::Sender<StatemapInstallationStatus>,
) -> Result<(), String>
// where
//     T: ReplicatorInstaller,
{
    // TODO: Pass the number of permits over an environment variable?
    let permit_count = 50;
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

                    // TODO: Implement proper logic to update the snapshot
                    let snapshot_version_to_update = Some(ver); //if ver % 5 == 0 { Some(ver) } else { None };

                    // error!("Going to install statemaps for version={ver} with snapshot={snapshot_version_to_update:?}");

                    match installer.install(statemaps, snapshot_version_to_update).await {
                        Ok(status) => {
                            let end_installation_time = start_installation_time.elapsed();
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
                            return Ok(());
                        }

                        Err(err) => {
                            error!(
                                "Installed failed for version={snapshot_version_to_update:?} with time={:?} error={err:?}",
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

                            return Err(err.to_string());
                        }
                    };
                });
            };
        };
    }
}
// $coverage:ignore-end
