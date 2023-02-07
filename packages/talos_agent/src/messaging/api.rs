use crate::api::CandidateData;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

// This should live in the external shared schema exported by Talos
#[derive(Debug, EnumString, Display)]
pub enum TalosMessageType {
    Candidate,
    Decision,
}

/// Kafka candidate message which will be published to Talos queue
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CandidateMessage {
    pub xid: String,
    pub agent: String,
    pub cohort: String,
    pub readset: Vec<String>,
    pub readvers: Vec<u64>,
    pub snapshot: u64,
    pub writeset: Vec<String>,
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

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum Decision {
    #[serde(rename = "committed")]
    Committed,
    #[serde(rename = "aborted")]
    Aborted,
}

/// Kafka message which will be published to Talos queue
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", tag = "_typ")]
pub struct DecisionMessage {
    pub xid: String,
    pub agent: String,
    pub cohort: String,
    pub decision: Decision,
    pub suffix_start: u64,
    pub version: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub safepoint: Option<u64>,
    // #[serde(skip_serializing_if = "Option::is_none")]
    // pub conflicts: Option<Vec<ConflictMessage>>,
}

/// The response of message publishing action, essentially this is
/// the result of communication with broker (not with Talos)
pub struct PublishResponse {
    pub partition: i32,
    pub offset: i64,
}

/// The publishing contract
#[async_trait]
pub trait Publisher {
    async fn send_message(&self, key: String, message: CandidateMessage) -> Result<PublishResponse, String>;
}

pub type PublisherType = dyn Publisher + Sync + Send;

/// The consuming contract
#[async_trait]
pub trait Consumer {
    async fn receive_message(&self) -> Option<Result<DecisionMessage, String>>;
}

pub type ConsumerType = dyn Consumer + Sync + Send;
