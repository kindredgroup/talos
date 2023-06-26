use crate::api::{CandidateData, StateMap};
use crate::messaging::errors::MessagingError;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

pub static HEADER_MESSAGE_TYPE: &str = "messageType";
pub static HEADER_AGENT_ID: &str = "certAgent";

// This should live in the external shared schema exported by Talos
#[derive(Debug, EnumString, Display, PartialEq)]
pub enum TalosMessageType {
    Candidate,
    Decision,
}

/// Kafka candidate message which will be published to Talos queue
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", tag = "_typ")]
pub struct CandidateMessage {
    pub xid: String,
    pub agent: String,
    pub cohort: String,
    pub readset: Vec<String>,
    pub readvers: Vec<u64>,
    pub snapshot: u64,
    pub writeset: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statemap: Option<StateMap>,
    pub published_at: i128,
}

impl CandidateMessage {
    pub fn new(agent_name: String, cohort_name: String, candidate: CandidateData, published_at: i128) -> Self {
        Self {
            xid: candidate.xid,
            agent: agent_name,
            cohort: cohort_name,
            readset: candidate.readset,
            readvers: candidate.readvers,
            snapshot: candidate.snapshot,
            writeset: candidate.writeset,
            statemap: candidate.statemap,
            published_at,
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
    #[serde(skip_deserializing)]
    pub agent: String,
    #[serde(skip_deserializing)]
    pub cohort: String,
    pub decision: Decision,
    #[serde(skip_deserializing)]
    pub suffix_start: u64,
    pub version: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub safepoint: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decided_at: Option<u64>,
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
    async fn send_message(&self, key: String, message: CandidateMessage) -> Result<PublishResponse, MessagingError>;
}

pub type PublisherType = dyn Publisher + Sync + Send;

/// The consuming contract
#[async_trait]
pub trait Consumer {
    async fn receive_message(&self) -> Option<Result<DecisionMessage, MessagingError>>;
}

pub type ConsumerType = dyn Consumer + Sync + Send;

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn new_constructors() {
        let _ = format!("{:?}", TalosMessageType::Decision);
        assert_eq!(TalosMessageType::from_str("Decision").unwrap(), TalosMessageType::Decision);

        let message = CandidateMessage::new(
            "agent".to_string(),
            "cohort".to_string(),
            CandidateData {
                xid: "xid".to_string(),
                readset: vec!["1".to_string()],
                readvers: vec![2_u64],
                snapshot: 1_u64,
                writeset: vec!["1".to_string()],
                statemap: None,
            },
            0,
        );

        let _ = format!("{:?}", message);
        assert_eq!(message, message.clone());

        let _ = format!("{:?}", Decision::Committed);

        assert!(if let Ok(v) = serde_json::to_string(&Decision::Committed) {
            v == *"\"committed\""
        } else {
            false
        });
        assert!(if let Ok(v) = serde_json::from_str::<Decision>("\"committed\"") {
            v == Decision::Committed
        } else {
            false
        });
        assert!(if let Ok(v) = serde_json::to_string(&Decision::Aborted) {
            v == *"\"aborted\""
        } else {
            false
        });
        assert!(if let Ok(v) = serde_json::from_str::<Decision>("\"aborted\"") {
            v == Decision::Aborted
        } else {
            false
        });
        let _ = format!("{:?}", Decision::Aborted.clone());
    }
}
// $coverage:ignore-end
