use std::collections::HashMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, PartialEq)]
pub enum CertificationCandidateCallbackResponse {
    Cancelled(String),
    Proceed(CertificationRequest),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CertificationRequest {
    pub candidate: CertificationCandidate,
    pub snapshot: u64,
    pub timeout_ms: u64,
}

fn default_text_plain_encoding() -> String {
    "text/plain".to_string()
}

fn default_application_json_encoding() -> String {
    "application/json".to_string()
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaAction {
    #[serde(default)]
    pub cluster: String,
    /// Topic to publish the payload
    pub topic: String,
    /// Key encoding to be used. Defaults to `text/plain`.
    #[serde(default = "default_text_plain_encoding")]
    pub key_encoding: String,
    /// Key for the message to publish.
    pub key: Option<String>,
    /// Optional if the message should be published to a specific partition.
    pub partition: Option<i32>,
    /// Optional headers while publishing.
    pub headers: Option<HashMap<String, String>>,
    /// Key encoding to be used. Defaults to `application/json`.
    #[serde(default = "default_application_json_encoding")]
    pub value_encoding: String,
    /// Payload to publish.
    pub value: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct CandidateOnCommitPublishActions {
    pub kafka: Vec<KafkaAction>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct CandidateOnCommitActions {
    pub publish: Option<CandidateOnCommitPublishActions>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CertificationCandidate {
    pub readset: Vec<String>,
    pub writeset: Vec<String>,
    pub readvers: Vec<u64>,
    pub statemaps: Option<Vec<HashMap<String, Value>>>,
    pub on_commit: Option<CandidateOnCommitActions>,
}

#[derive(Debug, Clone)]
pub struct OutOfOrderInstallRequest {
    pub xid: String,
    pub version: u64,
    pub safepoint: u64,
    pub statemaps: Vec<HashMap<String, Value>>,
}

#[derive(Debug, PartialEq, PartialOrd)]

pub enum OutOfOrderInstallOutcome {
    Installed,
    InstalledAlready,
    SafepointCondition,
}

#[async_trait]
pub trait OutOfOrderInstaller {
    async fn install(&self, install_item: OutOfOrderInstallRequest) -> Result<OutOfOrderInstallOutcome, String>;
}
