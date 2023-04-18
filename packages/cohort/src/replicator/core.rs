use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use talos_certifier::model::DecisionMessage;

type Version = u64;

#[derive(Debug, Clone)]
pub enum ReceiverMessage {
    Candidate(Version, CandidateMessage),
    Decision(Version, DecisionMessage),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum DecisionOutcome {
    // #[serde(rename = "committed")]
    Committed,
    // #[serde(rename = "aborted")]
    Aborted,
    Timedout,
    Undecided,
}

// #[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
// pub enum SuffixItemReplicationState {
//     Ready,
//     Picked,
//     Complete,
// }

// impl Default for SuffixItemReplicationState {
//     fn default() -> Self {
//         SuffixItemReplicationState::Ready
//     }
// }

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StatemapItem {
    pub action: String,
    pub payload: Value,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CandidateMessage {
    pub xid: String,
    pub agent: String,
    pub cohort: String,
    pub readset: Vec<String>,
    pub readvers: Vec<u64>,
    pub snapshot: u64,
    pub writeset: Vec<String>,

    #[serde(skip_deserializing)]
    pub version: u64,

    #[serde(skip_deserializing)]
    pub safepoint: Option<u64>,

    #[serde(skip_deserializing)]
    pub decision_outcome: Option<DecisionOutcome>,

    // #[serde(default = "SuffixItemReplicationState::default")]
    // pub replication_state: SuffixItemReplicationState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statemap: Option<Vec<HashMap<String, Value>>>,
}

pub trait ReplicatorSuffixItemTrait {
    fn get_safepoint(&self) -> &Option<u64>;
    fn get_statemap(&self) -> &Option<Vec<HashMap<String, Value>>>;
    fn set_safepoint(&mut self, safepoint: Option<u64>);
    fn set_decision_outcome(&mut self, decision_outcome: Option<DecisionOutcome>);
}

impl ReplicatorSuffixItemTrait for CandidateMessage {
    fn get_safepoint(&self) -> &Option<u64> {
        &self.safepoint
    }
    fn get_statemap(&self) -> &Option<Vec<HashMap<String, Value>>> {
        &self.statemap
    }
    fn set_safepoint(&mut self, safepoint: Option<u64>) {
        self.safepoint = safepoint
    }
    fn set_decision_outcome(&mut self, decision_outcome: Option<DecisionOutcome>) {
        self.decision_outcome = decision_outcome
    }
}
