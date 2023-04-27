use log::{info, warn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, marker::PhantomData};
use talos_certifier::{
    model::{CandidateMessage, DecisionMessageTrait},
    ports::MessageReciever,
    ChannelMessage,
};

use crate::replicator::utils::{get_filtered_batch, get_statemap_from_suffix_items};

use super::suffix::{ReplicatorSuffixItemTrait, ReplicatorSuffixTrait};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CandidateDecisionOutcome {
    Committed,
    Aborted,
    Timedout,
    Undecided,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StatemapItem {
    pub action: String,
    pub version: u64,
    pub payload: Value,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct ReplicatorCandidate {
    pub candidate: CandidateMessage,

    #[serde(skip_deserializing)]
    pub safepoint: Option<u64>,

    #[serde(skip_deserializing)]
    pub decision_outcome: Option<CandidateDecisionOutcome>,
}

impl From<CandidateMessage> for ReplicatorCandidate {
    fn from(value: CandidateMessage) -> Self {
        ReplicatorCandidate {
            candidate: value,
            safepoint: None,
            decision_outcome: None,
        }
    }
}

impl ReplicatorSuffixItemTrait for ReplicatorCandidate {
    fn get_safepoint(&self) -> &Option<u64> {
        &self.safepoint
    }
    fn get_statemap(&self) -> &Option<Vec<HashMap<String, Value>>> {
        &self.candidate.statemap
    }
    fn set_safepoint(&mut self, safepoint: Option<u64>) {
        self.safepoint = safepoint
    }
    fn set_decision_outcome(&mut self, decision_outcome: Option<CandidateDecisionOutcome>) {
        self.decision_outcome = decision_outcome
    }
}

pub struct Replicator<T, S, M>
where
    T: ReplicatorSuffixItemTrait,
    S: ReplicatorSuffixTrait<T> + std::fmt::Debug,
    M: MessageReciever<Message = ChannelMessage> + Send + Sync,
{
    pub receiver: M,
    pub suffix: S,
    _phantom: PhantomData<T>,
}

impl<T, S, M> Replicator<T, S, M>
where
    T: ReplicatorSuffixItemTrait,
    S: ReplicatorSuffixTrait<T> + std::fmt::Debug,
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
        self.suffix.set_decision_outcome(version, decision_outcome);
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
