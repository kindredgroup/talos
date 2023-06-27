// $coverage:ignore-start
use std::{
    fmt::Debug,
    time::{Duration, Instant},
};

use crate::replicator::{
    core::{Replicator, ReplicatorCandidate, ReplicatorChannel, StatemapItem},
    suffix::{ReplicatorSuffixItemTrait, ReplicatorSuffixTrait},
    utils::get_filtered_batch,
};

use log::{debug, info, warn};
use talos_certifier::{ports::MessageReciever, ChannelMessage};
use talos_suffix::SuffixItem;
use tokio::sync::mpsc;

pub async fn replicator_service<S, M>(
    statemaps_tx: mpsc::Sender<(Vec<StatemapItem>, Option<u64>)>,
    mut replicator_rx: mpsc::Receiver<ReplicatorChannel>,
    mut replicator: Replicator<ReplicatorCandidate, S, M>,
) -> Result<(), String>
where
    S: ReplicatorSuffixTrait<ReplicatorCandidate> + Debug,
    M: MessageReciever<Message = ChannelMessage> + Send + Sync,
{
    info!("Starting Replicator Service.... ");
    // let mut interval = tokio::time::interval(Duration::from_millis(1_000));

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


                        let instance = Instant::now();
                        let msg_batch_instance = Instant::now();
                        // get batch of items from suffix to install.
                        let items_option = replicator.suffix.get_message_batch_from(replicator.last_installing, None);
                        let msg_batch_instance_elapsed = msg_batch_instance.elapsed();

                        let msg_statemap_create = Instant::now();

                        let mut msg_statemap_create_elapsed = Duration::from_nanos(0);
                        if let Some(items) = items_option {
                            let msg_batch_instance_filter = Instant::now();
                            let filtered_message_batch = get_filtered_batch(items.iter().copied());
                            let msg_batch_instance_filter_elapsed = msg_batch_instance_filter.elapsed();

                            let filter_vec = filtered_message_batch.collect::<Vec<&SuffixItem<ReplicatorCandidate>>>();
                            let filtered_vec_len = filter_vec.len();
                            // generate the statemap from each item in batch.
                            if filtered_vec_len > 0 {

                                for b_item in filter_vec {

                                    if let Some(statemaps) = b_item.item.get_statemap() {
                                        let statemaps_to_install = statemaps.iter().map(|sm| {
                                            let key = sm.keys().next().unwrap().to_string();
                                            let payload = sm.get(&key).unwrap().clone();
                                            StatemapItem {
                                                action: key,
                                                payload,
                                                version: b_item.item_ver,
                                            }
                                        }).collect::<Vec<StatemapItem>>();

                                        msg_statemap_create_elapsed = msg_statemap_create.elapsed();

                                        // send for install.
                                        statemaps_tx.send((statemaps_to_install, Some(b_item.item_ver))).await.unwrap();
                                    }
                                }
                            }


                            let elapsed = instance.elapsed();
                            if let Some(last_item) = items.last() {
                                replicator.last_installing = last_item.item_ver;
                            }

                            // TODO: Remove TEMP_CODE and replace with proper metrics from feature/cohort-db-mock
                            if !items.is_empty() {
                                let first_version =  items.first().unwrap().item_ver;
                                let last_version =  items.last().unwrap().item_ver;
                                warn!("[CREATE_STATEMAP] Processed total of count={} from_version={first_version:?} to_version={last_version:?} with batch_create_time={:?}, filter_time={msg_batch_instance_filter_elapsed:?} and filter_len={filtered_vec_len} statemap_create_time={msg_statemap_create_elapsed:?} and total_time={elapsed:?} {}", items.len(), msg_batch_instance_elapsed, elapsed.as_nanos());
                            }
                        }

                    },
                }
            }
        }
        res = replicator_rx.recv() => {
            if let Some(result) = res {
                match result {
                    // 4. Remove the versions if installations are complete.
                    ReplicatorChannel::InstallationSuccess(vers) => {
                        let version = vers.last().unwrap().to_owned();
                        debug!("Installated successfully till version={version:?}");
                        // Mark the suffix item as installed.
                        replicator.suffix.set_item_installed(version);

                        replicator.last_installed = version;
                        // if all prior items are installed, then update the prune vers
                        replicator.suffix.update_prune_index(version);


                        // Prune suffix and update suffix head.
                        if replicator.suffix.get_suffix_meta().prune_index >= replicator.suffix.get_suffix_meta().prune_start_threshold {
                            replicator.suffix.prune_till_version(version).unwrap();
                        }

                        // commit the offset
                        replicator.receiver.commit(version).await.unwrap();
                    }
                }
            }
        }

        }
    }
}

// $coverage:ignore-end
