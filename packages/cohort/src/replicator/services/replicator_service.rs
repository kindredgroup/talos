// $coverage:ignore-start
use std::{fmt::Debug, time::Duration};

use crate::replicator::{
    core::{Replicator, ReplicatorChannel, StatemapItem},
    models::ReplicatorCandidate,
    suffix::ReplicatorSuffixTrait,
};

use log::{debug, error, info};
use talos_certifier::{ports::MessageReciever, ChannelMessage};
use time::OffsetDateTime;
use tokio::sync::mpsc;
pub struct ReplicatorServiceConfig {
    pub commit_frequency_ms: u64,
    pub enable_stats: bool,
}

pub async fn replicator_service<S, M>(
    statemaps_tx: mpsc::Sender<Vec<StatemapItem>>,
    mut replicator_rx: mpsc::Receiver<ReplicatorChannel>,
    mut replicator: Replicator<ReplicatorCandidate, S, M>,
    config: ReplicatorServiceConfig,
) -> Result<(), String>
where
    S: ReplicatorSuffixTrait<ReplicatorCandidate> + Debug,
    M: MessageReciever<Message = ChannelMessage> + Send + Sync,
{
    info!("Starting Replicator Service.... ");
    let mut interval = tokio::time::interval(Duration::from_millis(config.commit_frequency_ms));

    let mut total_items_send = 0;
    let mut total_items_processed = 0;
    let mut total_items_installed = 0;
    let mut time_first_item_created_start_ns: i128 = 0; //
    let mut time_last_item_send_end_ns: i128 = 0;
    let mut time_last_item_installed_ns: i128 = 0;

    loop {
        tokio::select! {
        // 1. Consume message.
        res = replicator.receiver.consume_message() => {
            if let Ok(Some(msg)) = res {

                // 2. Add/update to suffix.
                match msg {
                    // 2.1 For CM - Install messages on the version
                    ChannelMessage::Candidate( message) => {
                        let version = message.version;
                        replicator.process_consumer_message(version, message.into()).await;
                    },
                    // 2.2 For DM - Update the decision with outcome + safepoint.
                    ChannelMessage::Decision(decision_version, decision_message) => {
                        replicator.process_decision_message(decision_version, decision_message).await;

                        if total_items_processed == 0 {
                            time_first_item_created_start_ns = OffsetDateTime::now_utc().unix_timestamp_nanos();
                        }
                        // Get a batch of remaining versions with their statemaps to install.
                        let (all_versions_picked, statemaps_batch) = replicator.generate_statemap_batch();

                        total_items_send += statemaps_batch.len();
                        total_items_processed += all_versions_picked.len();

                        let statemap_batch_cloned = statemaps_batch.clone();
                        let versions_not_sent = all_versions_picked.into_iter().filter(|&v| {
                            !statemap_batch_cloned.iter().any(|(ver, _)| ver != &v)
                        });


                        // Send statemaps batch to
                        for (_, statemap_vec) in statemaps_batch {
                            statemaps_tx.send(statemap_vec).await.unwrap();
                        }

                        // These versions are decided but they are not send to Statemap installer as they are either aborted or don't have statemap
                        versions_not_sent.for_each(|version| {
                            replicator.suffix.set_item_installed(version);

                        });

                        time_last_item_send_end_ns = OffsetDateTime::now_utc().unix_timestamp_nanos();

                    },
                }
            }
        }
        // Commit offsets at interval.
        _ = interval.tick() => {
            if config.enable_stats {
                let duration_sec = Duration::from_nanos((time_last_item_send_end_ns - time_first_item_created_start_ns) as u64).as_secs_f32();
                let tps_send = total_items_send as f32 / duration_sec;
                let tps_processed = total_items_processed as f32 / duration_sec;


                let duration_installed_sec = Duration::from_nanos((time_last_item_installed_ns - time_first_item_created_start_ns) as u64).as_secs_f32();
                let tps_install = total_items_installed as f32 / duration_installed_sec;
                // let tps_install_feedback =

                error!("
                Replicator Stats:
                      processed             : tps={tps_processed:.3}    | count={total_items_processed}
                      send for install      : tps={tps_send:.3}    | count={total_items_send}
                      installed             : tps={tps_install:.3}    | count={total_items_installed}
                    \n ");
            }

            replicator.commit_till_last_installed().await;
        }
        // Receive feedback from installer.
        res = replicator_rx.recv() => {
                if let Some(result) = res {
                    match result {
                        // 4. Remove the versions if installations are complete.
                        ReplicatorChannel::InstallationSuccess(vers) => {

                            let version = vers.last().unwrap().to_owned();
                            debug!("Installated successfully till version={version:?}");
                            // Mark the suffix item as installed.
                            replicator.suffix.set_item_installed(version);

                            // // if all prior items are installed, then update the prune vers
                            replicator.suffix.update_prune_index(version);


                            // Prune suffix and update suffix head.
                            if replicator.suffix.get_suffix_meta().prune_index >= replicator.suffix.get_suffix_meta().prune_start_threshold {
                                replicator.suffix.prune_till_version(version).unwrap();
                            }
                            total_items_installed += 1;
                            time_last_item_installed_ns = OffsetDateTime::now_utc().unix_timestamp_nanos();


                        }
                        ReplicatorChannel::InstallationFailure(_) => {
                            // panic!("[panic panic panic] Installation Failed and replicator will panic and stop");
                        }
                    }
                }
            }

        }
    }
}

// $coverage:ignore-end
