use talos_certifier::{
    model::{CandidateMessage, DecisionMessageTrait},
    ports::MessageReciever,
    ChannelMessage,
};

use crate::replicator::core::StatemapItem;

use super::cohort_suffix::CohortSuffix;

pub struct CohortReplicator<M>
where
    M: MessageReciever<Message = ChannelMessage> + Send + Sync,
{
    latest_in_flight: Option<u64>,
    receiver: M,
    suffix: CohortSuffix,
}

impl<M> CohortReplicator<M>
where
    M: MessageReciever<Message = ChannelMessage> + Send + Sync,
{
    pub fn new(receiver: M, suffix: CohortSuffix) -> CohortReplicator<M> {
        CohortReplicator {
            receiver,
            suffix,
            latest_in_flight: None,
        }
    }

    fn process_consumer_message(&mut self, version: u64, message: CandidateMessage) {
        if version > 0 {
            if let Err(e) = self.suffix.insert(version, message.into()) {
                log::error!("Unable to insert version {} into suffix. Error: {}", version, e)
            }
        } else {
            log::warn!("Version 0 will not be inserted into suffix.")
        }
    }

    fn process_decision_message<D: DecisionMessageTrait>(&mut self, decision_version: u64, decision_message: D) {
        let version = decision_message.get_candidate_version();

        let decision_outcome = decision_message.get_decision().clone();

        self.suffix
            .update_decision(version, decision_version, decision_message.get_decided_at())
            .unwrap();
        self.suffix.set_decision_outcome(version, decision_outcome);
        self.suffix.set_safepoint(version, decision_message.get_safepoint());

        // If this is a duplicate, we mark it as installed (assuming the original version always comes first and therefore that will be installed.)
        if decision_message.is_duplicate() {
            self.suffix.set_item_installed(version);
        }
    }

    /// Return "true" if decision was received
    pub(crate) async fn receive(&mut self) -> bool {
        if let Ok(Some(message)) = self.receiver.consume_message().await {
            match message {
                ChannelMessage::Candidate(msg_candidate) => {
                    self.process_consumer_message(msg_candidate.version, msg_candidate);
                    false
                }

                ChannelMessage::Decision(version, msg_decision) => {
                    self.process_decision_message(version, msg_decision);
                    true
                }
            }
        } else {
            false
        }
    }

    pub async fn commit(&mut self) {
        if let Some(offset) = self.suffix.find_commit_offset(self.latest_in_flight) {
            if let Err(error) = self.receiver.commit().await {
                log::warn!("Unable to commit offset: {}. Error: {}", offset, error);
            }
        }
    }

    pub(crate) fn get_next_statemap(&mut self) -> Option<(Vec<StatemapItem>, u64, Option<i128>)> {
        if let Some(decided) = self.suffix.find_new_decided(self.latest_in_flight, true) {
            let has_statemap = decided.item.candidate.statemap.is_some();
            let is_committed = decided.item.safepoint.is_some();

            let statemaps = if !has_statemap || !is_committed {
                Vec::new()
            } else {
                let raw_statemaps = decided.item.candidate.statemap.unwrap();
                raw_statemaps
                    .iter()
                    .map(|map| {
                        let key = map.keys().next().unwrap().to_string();
                        let payload = map.get(&key).unwrap().clone();
                        StatemapItem {
                            action: key,
                            payload,
                            version: decided.item_ver,
                            safepoint: Some(100),
                        }
                    })
                    .collect::<Vec<StatemapItem>>()
            };

            self.suffix.set_item_in_flight(decided.item_ver);
            self.latest_in_flight = Some(decided.item_ver);

            Some((statemaps, decided.item_ver, decided.decided_at))
        } else {
            None
        }
    }

    pub(crate) fn update_suffix(&mut self, version: u64) -> Result<bool, String> {
        self.suffix.set_item_installed(version);
        self.latest_in_flight = None;
        self.suffix.update_prune_index(version);
        let meta = self.suffix.get_suffix_meta();
        if meta.prune_index >= meta.prune_start_threshold {
            let _ = self.suffix.prune_till_version(version).map_err(|suffix_error| format!("{}", suffix_error))?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
