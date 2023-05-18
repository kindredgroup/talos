use std::{collections::HashMap, sync::Arc};

use log::info;
use tokio::sync::{mpsc, Mutex};

use crate::replicator::statistics::core::{ReplicatorStatisticsChannelMessage, ReplicatorStatisticsItem};

pub fn get_stat_item(stats: &mut HashMap<u64, ReplicatorStatisticsItem>, version: u64) -> ReplicatorStatisticsItem {
    stats
        .get(&version)
        .map_or(ReplicatorStatisticsItem { version, ..Default::default() }, |i| i.clone())
}

pub async fn stats_service(
    stats: Arc<Mutex<HashMap<u64, ReplicatorStatisticsItem>>>,
    mut rx: mpsc::Receiver<ReplicatorStatisticsChannelMessage>,
    capture_stats: bool,
) {
    // early exit if stats is not required.
    if !capture_stats {
        info!("Not capturing stats as the flag is false....");
        return;
    }

    info!("Starting Replicator Statistics Service.... ");

    loop {
        if let Some(message) = rx.recv().await {
            let mut stats_obj = stats.lock().await;

            match message {
                ReplicatorStatisticsChannelMessage::CandidateReceivedTime(_, _) => {
                    todo!()
                    // let item = get_stat_item(&mut stats_obj, version);

                    // stats_obj.insert(
                    //     version,
                    //     ReplicatorStatisticsItem {
                    //         version,
                    //         candidate_received_time: Some(time),
                    //         ..item
                    //     },
                    // );
                    // }
                }
                ReplicatorStatisticsChannelMessage::DecisionReceivedTime(_, _) => todo!(),

                // Suffix related updates.
                ReplicatorStatisticsChannelMessage::SuffixInsertCandidateTime(version, time) => {
                    let item = get_stat_item(&mut stats_obj, version);

                    stats_obj.insert(
                        version,
                        ReplicatorStatisticsItem {
                            version,
                            suffix_insert_candidate_time: Some(time),
                            ..item
                        },
                    );
                }
                ReplicatorStatisticsChannelMessage::SuffixUpdateDecisionTime(version, time) => {
                    let item = get_stat_item(&mut stats_obj, version);

                    stats_obj.insert(
                        version,
                        ReplicatorStatisticsItem {
                            version,
                            suffix_decision_update_time: Some(time),
                            ..item
                        },
                    );
                }
                ReplicatorStatisticsChannelMessage::SuffixUpdateDecisionCommittedFlag(version, is_committed) => {
                    let item = get_stat_item(&mut stats_obj, version);

                    stats_obj.insert(
                        version,
                        ReplicatorStatisticsItem {
                            version,
                            is_committed_decision: Some(is_committed),
                            ..item
                        },
                    );
                }
                ReplicatorStatisticsChannelMessage::SuffixUpdateInstallFlagsTime(version, time) => {
                    let item = get_stat_item(&mut stats_obj, version);

                    stats_obj.insert(
                        version,
                        ReplicatorStatisticsItem {
                            version,
                            suffix_update_install_flag: Some(time),
                            ..item
                        },
                    );
                }

                //  Statemap related updates.
                ReplicatorStatisticsChannelMessage::StatemapBatchCreateTime(version, time) => {
                    let item = get_stat_item(&mut stats_obj, version);

                    let retries = if item.statemap_batch_create_time.is_some() {
                        item.statemap_install_retries + 1
                    } else {
                        item.statemap_install_retries
                    };

                    stats_obj.insert(
                        version,
                        ReplicatorStatisticsItem {
                            version,
                            statemap_batch_create_time: Some(time),
                            statemap_install_retries: retries,
                            ..item
                        },
                    );
                }
                ReplicatorStatisticsChannelMessage::StatemapBatchSize(version, size) => {
                    let item = get_stat_item(&mut stats_obj, version);

                    stats_obj.insert(
                        version,
                        ReplicatorStatisticsItem {
                            version,
                            statemap_batch_size: Some(size),
                            ..item
                        },
                    );
                }
                ReplicatorStatisticsChannelMessage::StatemapInstallationTime(version, time) => {
                    let item = get_stat_item(&mut stats_obj, version);

                    stats_obj.insert(
                        version,
                        ReplicatorStatisticsItem {
                            version,
                            statemap_install_time: Some(time),
                            ..item
                        },
                    );
                }
                ReplicatorStatisticsChannelMessage::StatemapInstallationFlag(version, is_install_success) => {
                    let item = get_stat_item(&mut stats_obj, version);

                    stats_obj.insert(
                        version,
                        ReplicatorStatisticsItem {
                            version,
                            is_statemap_install_success: Some(is_install_success),
                            ..item
                        },
                    );
                }
            }
        };
    }
}
