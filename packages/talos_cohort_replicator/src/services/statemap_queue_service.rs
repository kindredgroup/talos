// $coverage:ignore-start

use std::time::Duration;

use log::{debug, error, info};
use time::OffsetDateTime;
use tokio::sync::mpsc;

use crate::{
    callbacks::ReplicatorSnapshotProvider,
    core::{StatemapInstallState, StatemapInstallationStatus, StatemapInstallerHashmap, StatemapItem},
    errors::{ReplicatorError, ReplicatorErrorKind},
    models::StatemapInstallerQueue,
};

#[derive(Debug)]
pub struct StatemapQueueServiceConfig {
    pub queue_cleanup_frequency_ms: u64,
    pub enable_stats: bool,
}

impl Default for StatemapQueueServiceConfig {
    fn default() -> Self {
        Self {
            queue_cleanup_frequency_ms: 10_000,
            enable_stats: true,
        }
    }
}

pub async fn statemap_queue_service<S>(
    mut statemaps_rx: mpsc::Receiver<(u64, Vec<StatemapItem>)>,
    mut statemap_installation_rx: mpsc::Receiver<StatemapInstallationStatus>,
    installation_tx: mpsc::Sender<(u64, Vec<StatemapItem>)>,
    snapshot_api: S,
    config: StatemapQueueServiceConfig,
) -> Result<(), ReplicatorError>
where
    S: ReplicatorSnapshotProvider + Send + Sync,
{
    info!("Starting Installer Queue Service.... ");
    let mut cleanup_interval = tokio::time::interval(Duration::from_millis(config.queue_cleanup_frequency_ms));

    let mut installation_success_count = 0;
    let mut send_for_install_count = 0;
    let mut first_install_start: i128 = 0; //
    let mut last_install_end: i128 = 0; //  = OffsetDateTime::now_utc().unix_timestamp_nanos();

    let mut statemap_installer_queue = StatemapInstallerQueue::default();

    //Gets snapshot initial version from db.
    statemap_installer_queue.update_snapshot(snapshot_api.get_snapshot().await.map_err(|e| ReplicatorError {
        kind: ReplicatorErrorKind::Persistence,
        reason: "Unable to get initial snapshot".into(),
        cause: Some(e.to_string()),
    })?);

    loop {
        tokio::select! {
            statemap_batch_option = statemaps_rx.recv() => {

                if let Some((ver, statemaps)) = statemap_batch_option {

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
                        if send_for_install_count == 0 {
                            first_install_start = OffsetDateTime::now_utc().unix_timestamp_nanos();
                        }
                        send_for_install_count += 1;
                        installation_tx.send((key, statemap_installer_queue.queue.get(&key).unwrap().statemaps.clone())).await.unwrap();

                        // Update the status flag
                        statemap_installer_queue.update_queue_item_state(&key, StatemapInstallState::Inflight);
                    }
                }
            }
            Some(install_result) = statemap_installation_rx.recv() => {
                match install_result {
                    StatemapInstallationStatus::Success(key) => {

                        // installed successfully and will remove the item
                        statemap_installer_queue.update_queue_item_state(&key, StatemapInstallState::Installed);


                        if let Some(last_contiguous_install_item) = statemap_installer_queue.queue.iter().take_while(|(_, statemap_installer_item)| statemap_installer_item.state == StatemapInstallState::Installed).last(){
                            statemap_installer_queue.update_snapshot(last_contiguous_install_item.1.version) ;
                        };


                        installation_success_count += 1;
                        last_install_end = OffsetDateTime::now_utc().unix_timestamp_nanos();

                    },
                    StatemapInstallationStatus::Error(ver, error) => {
                        error!("Failed to install version={ver} due to error={error:?}");
                        // set the item back to awaiting so that it will be picked again for installation.
                        statemap_installer_queue.update_queue_item_state(&ver, StatemapInstallState::Awaiting);
                    },
                }

                let  items_to_install: Vec<u64> = statemap_installer_queue.get_versions_to_install();

                // Sends for installation.
                for key in items_to_install {
                    // Send for installation
                    if send_for_install_count == 0 {
                        first_install_start = OffsetDateTime::now_utc().unix_timestamp_nanos();
                    }
                    send_for_install_count += 1;
                    installation_tx.send((key, statemap_installer_queue.queue.get(&key).unwrap().statemaps.clone())).await.unwrap();

                    // Update the status flag
                    statemap_installer_queue.update_queue_item_state(&key, StatemapInstallState::Inflight);
                }
            }
            _ = cleanup_interval.tick() => {
                if config.enable_stats {
                    let duration_sec = Duration::from_nanos((last_install_end - first_install_start) as u64).as_secs_f32();
                    let tps = installation_success_count as f32 / duration_sec;

                    let awaiting_count = statemap_installer_queue.filter_items_by_state(StatemapInstallState::Awaiting).count();
                    let inflight_count = statemap_installer_queue.filter_items_by_state(StatemapInstallState::Inflight).count();
                    debug!("
                    Statemap Installer Queue Stats:
                        tps             : {tps:.3}
                        counts          :
                                        | success={installation_success_count}
                                        | awaiting_installs={awaiting_count}
                                        | inflight_count={inflight_count}
                        current snapshot: {}
                        \n ", statemap_installer_queue.snapshot_version);
                    //   last vers send to install : {last_item_send_for_install}

                    if config.enable_stats && awaiting_count > 0 && inflight_count == 0
                    {
                        let result = statemap_installer_queue.dbg_get_versions_to_install();
                        let final_items = result.installable_items;
                        let criteria_awaiting_state = &result.filter_steps_insights[0];
                        let criteria_snapshot_check = &result.filter_steps_insights[1];
                        let criteria_seriazable_check = &result.filter_steps_insights[2];

                        if let Some(first_item_to_fail_safepoint_check) = criteria_snapshot_check.filter_reject_items.first() {
                            debug!("\n\n
                +----------+-----------------------------------+----------------------------+
                | Total    | Items in queue                    | {}
                +----------+-----------------------------------+----------------------------+
                | Filter 1 | Items in AWAITING state           | {}
                | Filter 2 | Items whose safepoint <= snapshot | {}
                |          |                                   | Current snapshot={}
                |          |-----------------------------------+----------------------------+
                |          |  > First item to fail this check  | Version={}
                |          |                                   | Safepoint={:?}
                |          |-----------------------------------+----------------------------+
                | Filter 3 | Items serializable in the batch   | {}
                |          |  > Non serializable items count   | {}
                +----------+-----------------------------------+------+---------------------+
                | Total    | Items ready to install            | {}
                +----------+-----------------------------------+----------------------------+
                            \n\n",
                            criteria_awaiting_state.filter_enter_count,
                            criteria_awaiting_state.filter_exit_count,
                            criteria_snapshot_check.filter_exit_count,
                            statemap_installer_queue.snapshot_version,
                            first_item_to_fail_safepoint_check.version,
                            first_item_to_fail_safepoint_check.safepoint,
                            criteria_seriazable_check.filter_exit_count,
                            criteria_seriazable_check.filter_reject_items.len(),
                            final_items.len(),
                        );
                        }
                    }
                }

                statemap_installer_queue.remove_installed();
            }

        }
    }
}

// $coverage:ignore-end
