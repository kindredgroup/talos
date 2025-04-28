use core::fmt;
use std::collections::HashMap;
use std::fmt::Display;

use crate::api::{CandidateData, StateMap, TalosType};
use crate::messaging::errors::MessagingError;
use async_trait::async_trait;
use opentelemetry::Context;
use serde::{Deserialize, Serialize};
use serde_json::Value;
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_commit: Option<Box<Value>>,
    /// Cohort started certification
    #[serde(default)]
    pub certification_started_at: i128,
    /// The request for certification is created
    #[serde(default)]
    pub request_created_at: i128,
    /// Candidate published to kafka (agent time)
    pub published_at: i128,
}

impl CandidateMessage {
    pub fn new(
        agent_name: String,
        cohort_name: String,
        candidate: CandidateData,
        certification_started_at: i128,
        request_created_at: i128,
        published_at: i128,
    ) -> Self {
        Self {
            xid: candidate.xid,
            agent: agent_name,
            cohort: cohort_name,
            readset: candidate.readset,
            readvers: candidate.readvers,
            snapshot: candidate.snapshot,
            writeset: candidate.writeset,
            statemap: candidate.statemap,
            on_commit: candidate.on_commit,
            certification_started_at,
            request_created_at,
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

impl Display for Decision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", if *self == Self::Committed { "comitted" } else { "aborted" })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ConflictMessage {
    pub xid: String,
    pub version: u64,
    pub readset: Vec<String>,
    pub readvers: Vec<u64>,
    pub writeset: Vec<String>,
}

/// Kafka message which will be published to Talos queue
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
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
    pub safepoint: Option<u64>,
    pub conflicts: Option<Vec<ConflictMessage>>,
    pub metrics: Option<TxProcessingTimeline>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct TraceableDecision {
    pub decision: DecisionMessage,
    pub raw_span_context: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TxProcessingTimeline {
    pub certification_started: i128,
    pub request_created: i128,
    pub candidate_published: i128,
    pub candidate_received: i128,
    pub candidate_processing_started: i128,
    pub decision_created_at: i128,
    pub db_save_started: i128,
    pub db_save_ended: i128,
    #[serde(skip_serializing, skip_deserializing)]
    pub decision_received_at: i128,
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
    async fn send_message(
        &self,
        key: String,
        message: CandidateMessage,
        headers: Option<HashMap<String, String>>,
        parent_span_ctx: Option<Context>,
    ) -> Result<PublishResponse, MessagingError>;
}

pub type PublisherType = dyn Publisher + Sync + Send;

/// The consuming contract
#[async_trait]
pub trait Consumer {
    async fn receive_message(&self) -> Option<Result<TraceableDecision, MessagingError>>;
    fn get_talos_type(&self) -> TalosType;
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
                on_commit: None,
            },
            0,
            0,
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

    #[test]
    fn deserialise_using_defaults() {
        let json = r#"{
            "xid": "xid-1",
            "agent": "agent",
            "cohort": "cohort",
            "snapshot": 2,
            "readset": [ "3", "4" ],
            "readvers": [ 5, 5 ],
            "writeset": [ "6" ],
            "publishedAt": 1
        }"#;

        let deserialised: CandidateMessage = serde_json::from_str(json).unwrap();
        assert_eq!(deserialised.certification_started_at, 0);
        assert_eq!(deserialised.request_created_at, 0);
        assert_eq!(deserialised.published_at, 1);
        assert_eq!(deserialised.xid, String::from("xid-1"));
        assert_eq!(deserialised.agent, String::from("agent"));
        assert_eq!(deserialised.cohort, String::from("cohort"));
        assert_eq!(deserialised.snapshot, 2);
        assert_eq!(deserialised.readset, vec!(String::from("3"), String::from("4")));
        assert_eq!(deserialised.readvers, vec!(5, 5));
        assert_eq!(deserialised.writeset, vec!(String::from("6")));
    }
}
// $coverage:ignore-end
