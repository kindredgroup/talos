use std::{fmt::Debug, time::Duration};

use crate::replicator::core::{DecisionOutcome, StateMapItem};

use super::{
    core::{CandidateMessage, ReceiverMessage},
    suffix::SuffixDecisionTrait,
};
use log::{info, warn};
use talos_certifier::{model::DecisionMessageTrait, ports::MessageReciever};
use talos_suffix::{SuffixItem, SuffixTrait};

pub struct Replicator<S, M>
where
    S: SuffixTrait<CandidateMessage> + SuffixDecisionTrait<CandidateMessage> + Debug,
    M: MessageReciever<Message = ReceiverMessage> + Send + Sync,
{
    receiver: M,
    suffix: S,
}

impl<S, M> Replicator<S, M>
where
    S: talos_suffix::SuffixTrait<CandidateMessage> + SuffixDecisionTrait<CandidateMessage> + Debug,
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
                //      (a) Select only the messages that have statemap.
                //      (b) Filter out messages none of the writeset item exists in readset? (N/A)
                //      (c) Select only those messages where safepoint is > snapshot
                //      (d) Select only the operations that concern the cohort?
                //      (e) Finally send it to the state manager to do the updates.
                _ = interval.tick() => {
                    if let Some(batch) = self.suffix.get_message_batch() {

                        //TODO: Get the current snapshot from db.
                        let current_snapshot: u64 = 3;
                        let mut new_snapshot = 0; // initialise with old snapshot value.
                        if let Some(last_message) = batch.last() {
                            new_snapshot = last_message.item.version;
                        }

                        info!("Suffix Messages batch={:?} and new snapshot={new_snapshot}", batch.len());


                        // Filtering out messages that are not applicable.
                        let filtered_message_batch = batch.into_iter()
                            .filter(|m| m.item.safepoint.unwrap_or(0) > current_snapshot) // select only the messages that have safepoint > current_snapshot
                            .filter(|m| m.item.statemap.is_some()) // select only the messages that have statemap.
                            .collect::<Vec<&SuffixItem<CandidateMessage>>>()
                        ;

                        info!("Filtered Messages={:?} and new snapshot={new_snapshot}", filtered_message_batch);

                        let statemap_batch: Vec<StateMapItem> = filtered_message_batch.into_iter().fold(vec![], |mut acc, m| {
                            m.item.statemap.as_ref().unwrap().iter().for_each(|sm| {
                                let key = sm.keys().next().unwrap().to_string();
                                let payload = sm.get(&key).unwrap().clone();
                                acc.push(StateMapItem {
                                    action: key,
                                    payload
                                })});
                                acc
                            });

                            info!("Statemap_Batch={statemap_batch:?} ");

                            // Filter out actions not required by cohort

                            // Send to state manager
                            //  * What happens if update fails?
                            //  ** If retry also fails?

                            // 4. Mark the versions completed to be vaccuumed later (clean up/remove).
                    }
                }
            }
        }
    }
}
