use serde::{Deserialize, Serialize};
use talos_certifier::model::{CandidateMessage, Decision};

use crate::replicator::core::StatemapItem;

#[derive(Debug)]
pub struct StateMapWithVersion {
    pub statemap: Vec<StatemapItem>,
    pub version: u64,
}

#[derive(Debug, Clone)]
pub enum InstallOutcome {
    Success {
        version: u64,
        started_at: i128,
        finished_at: i128,
    },
    Error {
        version: u64,
        started_at: i128,
        finished_at: i128,
        error: String,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct ReplicatorCandidate2 {
    pub candidate: CandidateMessage,

    #[serde(skip_deserializing)]
    pub safepoint: Option<u64>,

    #[serde(skip_deserializing)]
    pub decision: Option<Decision>,
}

impl From<CandidateMessage> for ReplicatorCandidate2 {
    fn from(value: CandidateMessage) -> Self {
        ReplicatorCandidate2 {
            candidate: value,
            safepoint: None,
            decision: None,
        }
    }
}
