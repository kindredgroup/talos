use std::{collections::HashMap, sync::Arc};

use tokio::sync::{mpsc, Mutex};

use crate::replicator::statistics::core::{ReplicatorStatisticsChannelMessage, ReplicatorStatisticsItem};

pub fn get_stat_item(stats: &mut HashMap<u64, ReplicatorStatisticsItem>, version: u64) -> ReplicatorStatisticsItem {
    stats
        .get(&version)
        .map_or(ReplicatorStatisticsItem { version, ..Default::default() }, |i| i.clone())
}

pub async fn stats_service(stats: Arc<Mutex<HashMap<u64, ReplicatorStatisticsItem>>>, mut rx: mpsc::Receiver<ReplicatorStatisticsChannelMessage>) {
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
                ReplicatorStatisticsChannelMessage::SuffixInsertCandidate(version, time) => {
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
                ReplicatorStatisticsChannelMessage::SuffixUpdateDecision(version, time, is_committed) => {
                    let item = get_stat_item(&mut stats_obj, version);

                    stats_obj.insert(
                        version,
                        ReplicatorStatisticsItem {
                            version,
                            suffix_decision_update_time: Some(time),
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

                    stats_obj.insert(
                        version,
                        ReplicatorStatisticsItem {
                            version,
                            statemap_batch_create_time: Some(time),
                            ..item
                        },
                    );
                }
                ReplicatorStatisticsChannelMessage::StatemapInstallationTime(version, time, is_install_success) => {
                    let item = get_stat_item(&mut stats_obj, version);

                    stats_obj.insert(
                        version,
                        ReplicatorStatisticsItem {
                            version,
                            statemap_install_time: Some(time),
                            is_statemap_install_success: Some(is_install_success),
                            ..item
                        },
                    );
                }
            }
        };
    }
}
