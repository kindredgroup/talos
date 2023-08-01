use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use talos_certifier::model::CandidateMessage;

use crate::{core::CandidateDecisionOutcome, suffix::ReplicatorSuffixItemTrait};

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct ReplicatorCandidate {
    pub candidate: CandidateMessage,

    #[serde(skip_deserializing)]
    pub safepoint: Option<u64>,

    #[serde(skip_deserializing)]
    pub decision_outcome: Option<CandidateDecisionOutcome>,

    #[serde(skip_deserializing)]
    pub is_installed: bool,
}

impl From<CandidateMessage> for ReplicatorCandidate {
    fn from(candidate: CandidateMessage) -> Self {
        ReplicatorCandidate {
            candidate,
            safepoint: None,
            decision_outcome: None,
            is_installed: false,
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
    fn set_suffix_item_installed(&mut self) {
        self.is_installed = true
    }
    fn is_installed(&self) -> bool {
        self.is_installed
    }
}
