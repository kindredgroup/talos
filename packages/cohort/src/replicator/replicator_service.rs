use std::{fmt::Debug, time::Duration};

use crate::replicator::{
    core::DecisionOutcome,
    utils::{get_filtered_batch, get_statemap_from_suffix_items},
};

use super::{
    core::{CandidateMessage, ReceiverMessage},
    suffix::ReplicatorSuffixTrait,
};
use log::{info, warn};
use talos_certifier::{model::DecisionMessageTrait, ports::MessageReciever};

pub struct Replicator<S, M>
where
    S: ReplicatorSuffixTrait<CandidateMessage> + Debug,
    M: MessageReciever<Message = ReceiverMessage> + Send + Sync,
{
    receiver: M,
    suffix: S,
}

impl<S, M> Replicator<S, M>
where
    S: talos_suffix::SuffixTrait<CandidateMessage> + ReplicatorSuffixTrait<CandidateMessage> + Debug,
    M: MessageReciever<Message = ReceiverMessage> + Send + Sync,
{
    pub fn new(receiver: M, suffix: S) -> Self {
        Replicator { receiver, suffix }
    }

    pub async fn run(&mut self) {
        info!("Going to consume the message.... ");
        let mut interval = tokio::time::interval(Duration::from_millis(2_000));

        loop {
            tokio::select! {
                // 1. Consume message.
                res = self.receiver.consume_message() => {
                    if let Ok(Some(msg)) = res {

                        // 2. Add/update to suffix.
                        match msg {
                            // 2.1 For CM - Install messages on the version
                            ReceiverMessage::Candidate(version, message) => {
                                if version > 0 {
                                    self.suffix.insert(version, message).unwrap();
                                } else {
                                    warn!("Version 0 will not be inserted into suffix.")
                                }
                                // info!("Suffix Messages for {version}={:?}", self.suffix.get(version));
                            },
                            // 2.2 For DM - Update the decision with outcome + safepoint.
                            ReceiverMessage::Decision(decision_version, decision_message) => {
                                let version = decision_message.get_candidate_version();

                                let decision_outcome = match decision_message.decision{
                                    talos_certifier::model::Decision::Committed => Some(DecisionOutcome::Committed),
                                    talos_certifier::model::Decision::Aborted => Some(DecisionOutcome::Aborted),
                                };
                                // info!("In decision block for version={version} and decision_outcome={decision_outcome:?} for message={decision_message:?} ");

                                self.suffix.update_decision(version, decision_version).unwrap();
                                self.suffix.set_decision(version, decision_outcome);
                                self.suffix.set_safepoint(version, decision_message.get_safepoint());

                                // info!("Suffix Messages for {version}={:#?} after decision", self.suffix.get(version));

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
                    if let Some(batch) = self.suffix.get_message_batch() {

                        // Filtering out messages that are not applicable.
                        let batch_iter = batch.iter();
                        let filtered_message_batch = get_filtered_batch(batch_iter.copied()); //.collect::<Vec<&SuffixItem<CandidateMessage>>>();

                        // info!("Filtered Messages={:?} ", filtered_message_batch.collect::<Vec<&SuffixItem<CandidateMessage>>>());

                        // Create the statemap batch
                        let statemap_batch = get_statemap_from_suffix_items(filtered_message_batch);

                        info!("Statemap_Batch={statemap_batch:#?} ");

                            // Filter out actions not required by cohort

                            // Send to state manager/Bank API
                            // * Call fn to install statemaps in batch
                            // let result = self.installation_handler(statemap_batch).await;

                            // Update the snapshot.
                            // map.get("fn-name")(params)
                            //  **  What happens if update fails?
                            //  ***  If retry also fails?

                            // 4. Mark the versions completed to be vaccuumed later (clean up/remove).
                    }
                }
            }
        }
    }
}
