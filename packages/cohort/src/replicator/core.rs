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

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct StateMapItem {
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

    #[serde(skip_serializing_if = "Option::is_none")]
    pub statemap: Option<Vec<HashMap<String, Value>>>,
}
