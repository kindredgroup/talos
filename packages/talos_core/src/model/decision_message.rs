use certifier::certifier::Outcome;
// use super::CandidateMessage;
use serde::{Deserialize, Serialize};

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

    pub version: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub safepoint: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conflicts: Option<Vec<ConflictMessage>>,
    //TODO:- Add them in next iteration
    // pub time: String,
    // pub certifier: String,
    // pub processing_time: u16
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
        }
    }
}
