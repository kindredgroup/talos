// $coverage:ignore-start
use std::{fmt::Debug, future::Future, io::Error, time::Duration};

use super::{
    core::{Replicator, ReplicatorCandidate, StatemapItem},
    suffix::ReplicatorSuffixTrait,
};
use log::info;
use talos_certifier::{ports::MessageReciever, ChannelMessage};

pub async fn run_talos_replicator<S, M, F, Fut>(replicator: &mut Replicator<ReplicatorCandidate, S, M>, install_statemaps: F)
where
    S: ReplicatorSuffixTrait<ReplicatorCandidate> + Debug,
    M: MessageReciever<Message = ChannelMessage> + Send + Sync,
    Fut: Future<Output = Result<bool, Error>>,
    F: Fn(Vec<StatemapItem>) -> Fut,
{
    info!("Going to consume the message.... ");
    let mut interval = tokio::time::interval(Duration::from_millis(2_000));

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

                if let Some((statemap_batch, _last_item_vers)) = replicator.generate_statemap_batch() {

                    // Call fn to install statemaps in batch amd update the snapshot
                    let result = install_statemaps(statemap_batch).await;

                    // 4. Remove the versions if installations are complete.
                    if let Ok(res) = result {
                        if res {

                            //  4.1 Remove the versions
                            //  4.2 Update the head
                            //  4.3 Update the prune head
                            // let _prune_result = replicator.suffix.prune_till_version(last_item_vers);
                            //5. commit the version after pruning is successful
                          //   replicator.receiver.commit(last_item_vers).await.unwrap();
                        }

                    }
                }
            }
        }
    }
}
// $coverage:ignore-end
