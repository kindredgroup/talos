use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use talos_certifier::model::{CandidateDecisionOutcome, CandidateMessage};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StatemapItem {
    pub action: String,
    pub version: u64,
    pub payload: Value,
}

pub trait ReplicatorSuffixItemTrait {
    fn get_safepoint(&self) -> &Option<u64>;
    fn get_statemap(&self) -> &Option<Vec<HashMap<String, Value>>>;
    fn set_safepoint(&mut self, safepoint: Option<u64>);
    fn set_decision_outcome(&mut self, decision_outcome: Option<CandidateDecisionOutcome>);
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
