// $coverage:ignore-start

use std::{
    hash::BuildHasherDefault,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    model::requests::TransferRequest,
    replicator::core::{ReplicatorChannel, ReplicatorInstaller, StatemapItem},
};

use ahash::{HashSet, HashSetExt, RandomState};
use indexmap::{IndexMap, IndexSet};
use log::{debug, error, info, warn};
use rayon::prelude::*;
use time::OffsetDateTime;
use tokio::sync::{mpsc, Semaphore};
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

#[derive(Debug)]
pub enum StatemapInstallationStatus {
    Success(u64),
    Error(u64, String),
}

pub async fn installer_queue_service(
    mut statemaps_rx: mpsc::Receiver<Vec<(u64, Vec<StatemapItem>)>>,
    mut statemap_installation_rx: mpsc::Receiver<StatemapInstallationStatus>,
    installation_tx: mpsc::Sender<(u64, Vec<StatemapItem>)>,
) -> Result<(), String> {
    info!("Starting Installer Queue Service.... ");
    // let last_installed = 0_u64;
    let mut statemap_queue = IndexMap::<u64, StatemapInstallerHashmap, RandomState>::default();
    let mut interval = tokio::time::interval(Duration::from_millis(10_000));

    loop {
        tokio::select! {
            statemap_batch_option = statemaps_rx.recv() => {

                if let Some(batch) = statemap_batch_option {

                    let first_version = batch.first().unwrap().0;
                    let last_version = batch.last().unwrap().0;

                    let start_time_insert = Instant::now();
                    // Inserts the statemaps to the map
                    for (ver, statemap_batch) in batch {

                        let safepoint = if let Some(first_statemap) = statemap_batch.first() {
                            first_statemap.safepoint
                        } else {
                            None
                        };
                        statemap_queue.insert(ver, StatemapInstallerHashmap { statemaps: statemap_batch, version: ver, safepoint, status: StatemapInstallState::Awaiting });

                    }
                    let insert_elapsed = start_time_insert.elapsed();

                    // Gets the statemaps to send for installation.
                    let mut items_to_install: Vec<u64> = vec![];

                    // let last_installed_index = statemap_queue.get_index_of(&last_installed).unwrap_or(0);
                    // let awaiting_range = last_installed_index..;
                    let start_time_create_install_items = Instant::now();


                    let items = statemap_queue.get_range(..);

                    // TODO: Change to using ids instead of TransferRequest
                    let in_flight_accounts:HashSet<String> = statemap_queue.values().filter(|v| v.status == StatemapInstallState::Inflight).fold( HashSet::new(),|mut acc,v| {
                        v.statemaps.iter().for_each(|sm| {
                            let data: TransferRequest = serde_json::from_value(sm.payload.clone()).unwrap();
                            acc.insert(data.from);
                            acc.insert(data.to);
                        });
                        acc
                    });


                    if let Some(statemap_items) = items {

                        items_to_install = statemap_items
                        .values()
                        // Picking waiting items
                        .filter(|v| v.status == StatemapInstallState::Awaiting)
                        //  TODO: use ids instead of direct check on TransferRequest
                        //  Observed: Using filter instead of take_while, causes serious performance deterioration.
                        .take_while(|v| {
                            !v.statemaps.iter().any(|sm| {
                                let data: TransferRequest = serde_json::from_value(sm.payload.clone()).unwrap();
                                    // error!()

                                    let res = in_flight_accounts.contains(&data.from) || in_flight_accounts.contains(&data.to);
                                    if res {
                                        error!("For version {} , found either {} or {} in an existing in-flight txn", v.version, data.from, data.to);
                                    }
                                    res
                                 })
                        })
                        // filter out the ones that can't be serialized
                        .filter_map(|v| {
                            // If no safepoint, this could be a abort item and is safe to install as statemap will be empty.
                            let Some(safepoint) = v.safepoint else {
                                return Some(v.version);
                            };

                            // If there is no version matching the safepoint, then it is safe to install
                            if !statemap_queue.contains_key(&safepoint) {
                                return Some(v.version);
                            };

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
                        installation_tx.send((key, statemap_queue.get(&key).unwrap().statemaps.clone())).await.unwrap();

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
                        let end_time_success = start_time_success.elapsed();
                        // error!("(Statemap successfully installed) for version={key} in {end_time_success:?}");
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

                let installed_keys = statemap_queue.par_values().filter_map(|v| {if v.status == StatemapInstallState::Installed {
                    Some(v.version)
                } else {
                    None
                }})
                .collect::<Vec<u64>>();

                installed_keys.iter().for_each(|key| {
                  statemap_queue.shift_remove(key);
                })
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
    let semaphore = Arc::new(Semaphore::new(50));

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
                    let snapshot_version_to_update = if ver % 5 == 0 { Some(ver) } else { None };

                    // error!("Going to install statemaps for version={ver} with snapshot={snapshot_version_to_update:?}");

                    match installer.install(statemaps, snapshot_version_to_update).await {
                        Ok(true) => {
                            let end_installation_time = start_installation_time.elapsed();
                            // error!("[installation_service] Installed successfully version={ver} in {end_installation_time:?}");
                            replicator_tx_clone.send(ReplicatorChannel::InstallationSuccess(vec![ver])).await.unwrap();
                            statemap_installation_tx_clone.send(StatemapInstallationStatus::Success(ver)).await.unwrap();
                            drop(permit);
                            return Ok(());
                        }
                        Ok(false) => {
                            // Do nothing if result is false.
                            drop(permit);
                            error!(
                                "Installed failed for version={snapshot_version_to_update:?} with time ={:?}",
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

                            return Ok(());
                        }
                        Err(err) => {
                            error!(
                                "Installed failed for version={snapshot_version_to_update:?} with time={:?} error={err:?}",
                                start_installation_time.elapsed()
                            );
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
