// use super::CandidateMessage;
use serde::{Deserialize, Serialize};

use crate::certifier::Outcome;

use super::{candidate_message::CandidateMessage, metrics::TxProcessingTimeline};

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum Decision {
    #[serde(rename = "committed")]
    Committed,
    #[serde(rename = "aborted")]
    Aborted,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase", tag = "_typ")]
pub struct DecisionMessage {
    // UNIQUENESS FIELD
    pub xid: String,
    // DATA FIELDS
    pub agent: String,
    pub cohort: String,
    pub decision: Decision,
    pub suffix_start: u64,

    /// the version for which the decision was made.
    pub version: u64,
    /// If a duplicate was found on XDB, this field will hold the new duplicate candidate
    /// message's version.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duplicate_version: Option<u64>,
    /// Safepoint when decision is `committed`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub safepoint: Option<u64>,
    /// If decision is `aborted`.
    ///     - when some value, it is the version which had conflict and caused the abort by rule 3.
    ///     - when none, that means the abort was due to rule 2.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conflict_version: Option<u64>,

    pub metrics: TxProcessingTimeline,
}

pub trait DecisionMessageTrait {
    fn get_candidate_version(&self) -> u64;
    fn get_safepoint(&self) -> Option<u64>;
    fn get_decision(&self) -> &Decision;
    fn is_duplicate(&self) -> bool;
    fn get_decided_at(&self) -> i128;
}

impl DecisionMessageTrait for DecisionMessage {
    fn get_candidate_version(&self) -> u64 {
        match self.duplicate_version {
            Some(ver) => ver,
            None => self.version,
        }
    }
    fn get_safepoint(&self) -> Option<u64> {
        self.safepoint
    }
    fn get_decision(&self) -> &Decision {
        &self.decision
    }

    fn is_duplicate(&self) -> bool {
        self.duplicate_version.is_some()
    }

    fn get_decided_at(&self) -> i128 {
        self.metrics.decision_created_at
    }
}

impl DecisionMessage {
    pub fn new(candidate_message: &CandidateMessage, outcome: Outcome, suffix_start: u64) -> Self {
        let CandidateMessage {
            xid, agent, cohort, version, ..
        } = candidate_message;

        let (decision, safepoint, conflict_version) = match outcome {
            Outcome::Commited { discord: _, safepoint } => (Decision::Committed, Some(safepoint), None),
            Outcome::Aborted { version, discord: _ } => (Decision::Aborted, None, version),
        };

        Self {
            xid: xid.clone(),
            agent: agent.clone(),
            cohort: cohort.clone(),
            decision,
            suffix_start,
            version: *version,
            safepoint,
            conflict_version,
            duplicate_version: None,
            metrics: TxProcessingTimeline::default(),
        }
    }
}
