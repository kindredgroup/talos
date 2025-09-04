use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    core::CandidateDecisionOutcome,
    events::{EventTimingsMap, EventTimingsTrait, ReplicatorCandidateEvent},
    suffix::ReplicatorSuffixItemTrait,
};

use super::candidate_message::ReplicatorCandidateMessage;

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct ReplicatorCandidate {
    pub candidate: ReplicatorCandidateMessage,

    #[serde(skip_deserializing)]
    pub safepoint: Option<u64>,

    #[serde(skip_deserializing)]
    pub decision_outcome: Option<CandidateDecisionOutcome>,

    #[serde(skip_deserializing)]
    pub is_installed: bool,

    // pub event_timings: ReplicatorCandidateTimings,
    pub event_timings: EventTimingsMap,
}

impl From<ReplicatorCandidateMessage> for ReplicatorCandidate {
    fn from(candidate: ReplicatorCandidateMessage) -> Self {
        ReplicatorCandidate {
            candidate,
            safepoint: None,
            decision_outcome: None,
            is_installed: false,
            event_timings: HashMap::new(),
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

impl EventTimingsTrait for ReplicatorCandidate {
    fn record_event(&mut self, event: ReplicatorCandidateEvent, ts_ns: i128) {
        self.event_timings.insert(event, ts_ns);
    }
    fn get_event_timestamp(&self, event: ReplicatorCandidateEvent) -> Option<i128> {
        self.event_timings.get(&event).copied()
    }
    fn get_all_timings(&self) -> EventTimingsMap {
        self.event_timings.clone()
    }
}
