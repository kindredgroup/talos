// use super::CandidateMessage;
use serde::{Deserialize, Serialize};

use crate::certifier::Outcome;

use super::candidate_message::CandidateMessage;

// {
//     "xid": "38c729ec-dc28-4da4-9b0b-d3ec136a30ff",
//     "decision": "aborted",
//     "version": 142,
//     "time": "2021-08-02T11:21:34.523Z",
//     "agent":"qa-talos-agent-5233",
//     "certifier": "qa-talos-certifier-6233",
//     "cohort": "qa-chimera-verification-2343",
//     "processingTime": 0.46,
//     "suffixStart": 34, // the earliest transaction in the suffix, for debugging
//     "conflicts": [     // empty if txn was aborted by rule R2; nonempty is aborted by R3
//       {
//         "xid": "c592af34-1e47-42b1-b636-00e4e7d55817",
//         "version": 139,
//         "writeset": [
//           "ksp:coupon.1:123e4567-e89b-12d3-a456-426614174000",
//           "ksp:bet.1:123e4567-e89b-12d3-a456-426614174000:0",
//           "ksp:bet.1:123e4567-e89b-12d3-a456-426614174000:1"
//         ]
//       }
//     ]
//   }

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct ConflictMessage {
    pub xid: String,
    pub version: u64,
    pub readset: Vec<String>,
    pub readvers: Vec<u64>,
    pub writeset: Vec<String>,
}

impl From<CandidateMessage> for ConflictMessage {
    fn from(candidate_message: CandidateMessage) -> Self {
        let CandidateMessage {
            xid,
            readset,
            readvers,
            writeset,
            version,
            ..
        } = candidate_message;

        ConflictMessage {
            xid,
            version,
            readset,
            readvers,
            writeset,
        }
    }
}

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

    // the version for which this decision is made.
    pub version: u64,
    /// if duplicate is found on XDB, this field will hold the new duplicate candidate
    /// message's version.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duplicate_version: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub safepoint: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conflicts: Option<Vec<ConflictMessage>>,

    pub can_published_at: i128,
    pub can_received_at: i128,
    pub can_process_start: i128,
    pub can_process_end: i128,
    pub created_at: i128,
    pub db_start: i128,
    pub db_end: i128,
    pub received_at: i128,
    //TODO:- Add them in next iteration
    // pub time: String,
    // pub certifier: String,
    // pub processing_time: u16
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
        self.created_at
    }
}

impl DecisionMessage {
    pub fn new(candidate_message: &CandidateMessage, conflict_candidate: Option<CandidateMessage>, outcome: Outcome, suffix_start: u64) -> Self {
        let CandidateMessage {
            xid, agent, cohort, version, ..
        } = candidate_message;

        let (decision, safepoint, conflicts) = match outcome {
            Outcome::Commited { discord: _, safepoint } => (Decision::Committed, Some(safepoint), None as Option<Vec<ConflictMessage>>),
            Outcome::Aborted { version: _, discord: _ } => (
                Decision::Aborted,
                None,
                if conflict_candidate.is_none() {
                    None
                } else {
                    Some(vec![conflict_candidate.unwrap().into()])
                },
            ),
        };

        Self {
            xid: xid.clone(),
            agent: agent.clone(),
            cohort: cohort.clone(),
            decision,
            suffix_start,
            version: *version,
            safepoint,
            conflicts,
            duplicate_version: None,
            can_published_at: candidate_message.published_at,
            can_received_at: candidate_message.received_at,
            can_process_start: 0, // set later
            can_process_end: 0,   // set later
            created_at: 0,
            db_start: 0,
            db_end: 0,
            received_at: 0,
        }
    }
}
