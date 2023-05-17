// $coverage:ignore-start
use std::{
    fmt::Debug,
    time::{Duration, Instant},
};

use crate::replicator::{
    core::{Replicator, ReplicatorCandidate, ReplicatorChannel, StatemapItem},
    statistics::core::ReplicatorStatisticsChannelMessage,
    suffix::ReplicatorSuffixTrait,
};

use log::{debug, info};
use talos_certifier::{
    model::{Decision, DecisionMessageTrait},
    ports::MessageReciever,
    ChannelMessage,
};
use tokio::sync::mpsc;

pub async fn replicator_service<S, M>(
    statemaps_tx: mpsc::Sender<(Vec<StatemapItem>, Option<u64>)>,
    mut replicator_rx: mpsc::Receiver<ReplicatorChannel>,
    mut replicator: Replicator<ReplicatorCandidate, S, M>,
    statistics_tx: Option<mpsc::Sender<ReplicatorStatisticsChannelMessage>>,
) -> Result<(), String>
where
    S: ReplicatorSuffixTrait<ReplicatorCandidate> + Debug,
    M: MessageReciever<Message = ChannelMessage> + Send + Sync,
{
    info!("Starting Replicator Service.... ");
    let mut interval = tokio::time::interval(Duration::from_millis(1_000));

    let capture_stats = statistics_tx.is_some();

    loop {
        tokio::select! {
        // 1. Consume message.
        res = replicator.receiver.consume_message() => {
            if let Ok(Some(msg)) = res {

                // 2. Add/update to suffix.
                match msg {
                    // 2.1 For CM - Install messages on the version
                    ChannelMessage::Candidate( message) => {
                        let stats_suffix_insert_time = Instant::now();

                        let version = message.version;
                        replicator.process_consumer_message(version, message.into()).await;

                        let stats_suffix_insert_time_elapse = stats_suffix_insert_time.elapsed();
                        if capture_stats {
                            let _ = statistics_tx.as_ref().unwrap().send(ReplicatorStatisticsChannelMessage::SuffixInsertCandidate(version, stats_suffix_insert_time_elapse.as_nanos())).await;
                        };
                    },
                    // 2.2 For DM - Update the decision with outcome + safepoint.
                    ChannelMessage::Decision(decision_version, decision_message) => {
                        let stats_suffix_insert_time = Instant::now();

                        let version = decision_message.get_candidate_version();
                        let is_committed = decision_message.get_decision() == &Decision::Committed;

                        replicator.process_decision_message(decision_version, decision_message).await;

                        let stats_suffix_insert_time_elapse = stats_suffix_insert_time.elapsed();
                        if capture_stats {
                            let _ = statistics_tx.as_ref().unwrap().send(ReplicatorStatisticsChannelMessage::SuffixUpdateDecision(version, stats_suffix_insert_time_elapse.as_nanos(), is_committed)).await;
                        };

                    },
                }
            }
        }
        // 3. At set interval send the batch to be installed.
        //  3.1 Derive the new snapshot.
        //  3.2 Create batch of valid statemap instructions.
        //      (a) Select only the messages that have safepoint (i.e candidate messages with committed decisions).
        //      (b) Select only the messages that have statemap.
        //      (c) Send it to the state manager to do the updates.
        _ = interval.tick() => {

            let stats_suffix_insert_time = Instant::now();

            if let (Some(statemap_batch), version_option) = replicator.generate_statemap_batch() {
                if version_option.is_some() {

                    let stats_suffix_insert_time_elapse = stats_suffix_insert_time.elapsed();
                    if capture_stats {
                        let _ = statistics_tx.as_ref().unwrap().send(ReplicatorStatisticsChannelMessage::StatemapBatchCreateTime(version_option.unwrap(), stats_suffix_insert_time_elapse.as_nanos())).await;
                    };

                    info!("Statemap batch in replicator_service is ={statemap_batch:?}");

                    debug!("Getting ready to send the statemap for installation statemap_batch={statemap_batch:?} with version={version_option:?}");

                    let Err(error) = statemaps_tx.send((statemap_batch, version_option)).await else {
                        debug!("Send successfully over statemap channel");
                        continue;
                    };

                    return Err(error.to_string());


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

                        let stats_suffix_insert_time = Instant::now();

                        // Mark the suffix item as installed.
                        replicator.suffix.set_item_installed(version);

                        let stats_suffix_insert_time_elapse = stats_suffix_insert_time.elapsed();
                        if capture_stats {
                            let _ = statistics_tx.as_ref().unwrap().send(ReplicatorStatisticsChannelMessage::SuffixUpdateInstallFlagsTime(version, stats_suffix_insert_time_elapse.as_nanos())).await;
                        };
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

// pub async fn run_talos_replicator<S, M, T>(replicator: &mut Replicator<ReplicatorCandidate, S, M>, statemap_installer: &mut T)
// where
//     S: ReplicatorSuffixTrait<ReplicatorCandidate> + Debug,
//     M: MessageReciever<Message = ChannelMessage> + Send + Sync,
//     T: ReplicatorInstaller,
// {
//     info!("Going to consume the message.... ");
//     let mut interval = tokio::time::interval(Duration::from_millis(200));

//     loop {
//         tokio::select! {
//             // 1. Consume message.
//             res = replicator.receiver.consume_message() => {
//                 if let Ok(Some(msg)) = res {

//                     // 2. Add/update to suffix.
//                     match msg {
//                         // 2.1 For CM - Install messages on the version
//                         ChannelMessage::Candidate( message) => {
//                             let version = message.version;
//                             replicator.process_consumer_message(version, message.into()).await;
//                         },
//                         // 2.2 For DM - Update the decision with outcome + safepoint.
//                         ChannelMessage::Decision(decision_version, decision_message) => {
//                             replicator.process_decision_message(decision_version, decision_message).await;

//                         },
//                     }
//                 }
//             }
//             // 3. At set interval send the batch to be installed.
//             //  3.1 Derive the new snapshot.
//             //  3.2 Create batch of valid statemap instructions.
//             //      (a) Select only the messages that have safepoint (i.e candidate messages with committed decisions).
//             //      (b) Select only the messages that have statemap.
//             //      (c) Send it to the state manager to do the updates.
//             _ = interval.tick() => {

//                 if let (Some(statemap_batch), version_option) = replicator.generate_statemap_batch() {
//                     if version_option.is_some() {

//                         info!("Statemap batch in replicator_service is ={statemap_batch:?}");
//                         // let version = statemap_batch.iter().last().unwrap().version;
//                         // Call fn to install statemaps in batch amd update the snapshot
//                         let version = version_option.unwrap();

//                         let result = statemap_installer.install(statemap_batch, version_option).await;

//                         info!("Installation result ={result:?}");

//                         // 4. Remove the versions if installations are complete.
//                         if let Ok(res) = result {
//                             if res {

//                                 // Mark the suffix item as installed.
//                                 replicator.suffix.set_item_installed(version);
//                                 // if all prior items are installed, then update the prune vers
//                                 replicator.suffix.update_prune_index(version);

//                                 // Prune suffix and update suffix head.
//                                 if replicator.suffix.get_suffix_meta().prune_index >= replicator.suffix.get_suffix_meta().prune_start_threshold {
//                                     replicator.suffix.prune_till_version(version).unwrap();
//                                 }

//                                 // commit the offset
//                                 replicator.receiver.commit(version).await.unwrap();
//                             }

//                         }
//                     }
//                 }
//             }
//         }
//     }
// }

// $coverage:ignore-end
