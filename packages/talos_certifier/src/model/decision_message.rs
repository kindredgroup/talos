// use super::CandidateMessage;
use serde::{Deserialize, Serialize};
use strum::Display;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::{certifier::Outcome, core::CandidateMessageBaseTrait};

use super::metrics::TxProcessingTimeline;

pub const DEFAULT_DECISION_MESSAGE_VERSION: u64 = 1_u64;

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, Display)]
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

    /// timestamp when certification/decision was made.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<String>,

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
    fn is_abort(&self) -> bool;
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

    fn is_abort(&self) -> bool {
        self.decision == Decision::Aborted
    }

    fn is_duplicate(&self) -> bool {
        self.duplicate_version.is_some()
    }

    fn get_decided_at(&self) -> i128 {
        self.metrics.decision_created_at
    }
}

impl DecisionMessage {
    pub fn new<C: CandidateMessageBaseTrait>(candidate_message: &C, outcome: Outcome, suffix_start: u64) -> Self {
        let xid = candidate_message.get_xid().to_string();
        let agent = candidate_message.get_agent().to_string();
        let cohort = candidate_message.get_cohort().to_string();
        let version = candidate_message.get_version();

        let (decision, safepoint, conflict_version) = match outcome {
            Outcome::Commited { discord: _, safepoint } => (Decision::Committed, Some(safepoint), None),
            Outcome::Aborted { version, discord: _ } => (Decision::Aborted, None, version),
        };

        let time = OffsetDateTime::now_utc().format(&Rfc3339).ok();

        Self {
            xid,
            agent,
            cohort,
            time,
            decision,
            suffix_start,
            version,
            safepoint,
            conflict_version,
            duplicate_version: None,
            metrics: TxProcessingTimeline::default(),
        }
    }
}
