use serde::{Deserialize, Serialize};
use crate::api::CandidateData;

/// Kafka message which will be published to talos queue
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CandidateMessage {
    pub xid: String,
    pub agent: String,
    pub cohort: String,
    pub readset: Vec<String>,
    pub readvers: Vec<u64>,
    pub snapshot: u64,
    pub writeset: Vec<u64>,
}

impl CandidateMessage {
    pub fn new(agent_name: String, cohort_name: String, candidate: CandidateData) -> Self {
        Self {
            xid: candidate.xid,
            agent: agent_name,
            cohort: cohort_name,
            readset: candidate.readset,
            readvers: candidate.readvers,
            snapshot: candidate.snapshot,
            writeset: candidate.writeset,
        }
    }
}

/// The response of message publishing action, essentially this is
/// the result of communication wil broker (not with Talos)
pub struct PublishResponse {
    pub partition: i32,
    pub offset: i64,
}

/// The publishing contract
pub trait Publisher {
    fn send_message(&self, key: String, msg: CandidateMessage) -> Result<PublishResponse, String>;
}
