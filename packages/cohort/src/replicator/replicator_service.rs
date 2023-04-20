use std::{fmt::Debug, marker::PhantomData, time::Duration};

use crate::replicator::utils::{get_filtered_batch, get_statemap_from_suffix_items};

use super::{
    core::{ReplicatorCandidate, ReplicatorSuffixItemTrait, StatemapItem},
    suffix::ReplicatorSuffixTrait,
};
use log::{info, warn};
use talos_certifier::{
    model::{CandidateDecisionOutcome, DecisionMessageTrait},
    ports::MessageReciever,
    ChannelMessage,
};

pub struct Replicator<T, S, M>
where
    T: ReplicatorSuffixItemTrait,
    S: ReplicatorSuffixTrait<T> + Debug,
    M: MessageReciever<Message = ChannelMessage> + Send + Sync,
{
    receiver: M,
    suffix: S,
    _phantom: PhantomData<T>,
}

impl<T, S, M> Replicator<T, S, M>
where
    T: ReplicatorSuffixItemTrait,
    S: ReplicatorSuffixTrait<T> + Debug,
    M: MessageReciever<Message = ChannelMessage> + Send + Sync,
{
    pub fn new(receiver: M, suffix: S) -> Self {
        Replicator {
            receiver,
            suffix,
            _phantom: PhantomData,
        }
    }

    pub(crate) async fn process_consumer_message(&mut self, version: u64, message: T) {
        if version > 0 {
            self.suffix.insert(version, message).unwrap();
        } else {
            warn!("Version 0 will not be inserted into suffix.")
        }
    }

    pub(crate) async fn process_decision_message<D: DecisionMessageTrait>(&mut self, decision_version: u64, decision_message: D) {
        let version = decision_message.get_candidate_version();

        let decision_outcome = match decision_message.get_decision() {
            talos_certifier::model::Decision::Committed => Some(CandidateDecisionOutcome::Committed),
            talos_certifier::model::Decision::Aborted => Some(CandidateDecisionOutcome::Aborted),
        };
        self.suffix.update_decision(version, decision_version).unwrap();
        self.suffix.set_decision(version, decision_outcome);
        self.suffix.set_safepoint(version, decision_message.get_safepoint());
    }

    pub(crate) fn generate_statemap_batch(&self) -> Option<(Vec<StatemapItem>, u64)> {
        let Some(batch) = self.suffix.get_message_batch() else {
            return None;
        };

        // Filtering out messages that are not applicable.
        let filtered_message_batch = get_filtered_batch(batch.iter().copied()); //.collect::<Vec<&SuffixItem<CandidateMessage>>>();

        // Create the statemap batch
        let statemap_batch = get_statemap_from_suffix_items(filtered_message_batch);

        info!("Statemap_Batch={statemap_batch:#?} ");

        let last_item = batch.iter().last();
        let last_item_vers = last_item.unwrap().item_ver;
        Some((statemap_batch, last_item_vers))
    }
}

pub async fn run_talos_replicator<S, M, F>(replicator: &mut Replicator<ReplicatorCandidate, S, M>, install_statemaps: F)
where
    S: ReplicatorSuffixTrait<ReplicatorCandidate> + Debug,
    M: MessageReciever<Message = ChannelMessage> + Send + Sync,
    F: Fn(Vec<StatemapItem>) -> bool,
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

                if let Some((statemap_batch, last_item_vers)) = replicator.generate_statemap_batch() {

                    // Call fn to install statemaps in batch amd update the snapshot
                    let result = install_statemaps(statemap_batch);

                    // 4. Remove the versions if installations are complete.
                    if result {
                        //  4.1 Remove the versions
                        //  4.2 Update the head
                        //  4.3 Update the prune head
                        let _prune_result = replicator.suffix.prune_till_version(last_item_vers);

                      //5. commit the version after pruning is successful
                    //   replicator.receiver.commit(last_item_vers).await.unwrap();
                    }
                }
            }
        }
    }
}
