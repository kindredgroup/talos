use crate::agent::errors::AgentError;
use crate::messaging::api::{ConflictMessage, Decision};
use crate::metrics::model::MetricsReport;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;

///
/// Data structures and interfaces exposed to agent client
///

pub type StateMap = Vec<HashMap<String, Value>>;

/// The main candidate payload
#[derive(Clone, Debug)]
pub struct CandidateData {
    pub xid: String,
    pub readset: Vec<String>,
    pub readvers: Vec<u64>,
    pub snapshot: u64,
    pub writeset: Vec<String>,
    pub statemap: Option<StateMap>,
    pub on_commit: Option<Box<Value>>,
}

/// The data input from client to agent
#[derive(Clone, Debug)]
pub struct CertificationRequest {
    pub message_key: String,
    pub candidate: CandidateData,
    pub timeout: Option<Duration>,
    pub headers: Option<HashMap<String, String>>,
    pub certification_started_at: i128,
    pub request_created_at: i128,
}

/// The data output from agent to client
#[derive(Clone, Debug)]
pub struct CertificationResponse {
    pub xid: String,
    pub decision: Decision,
    pub version: u64,
    pub safepoint: Option<u64>,
    pub conflict: Option<ConflictMessage>,
}

#[derive(Clone, Debug)]
pub struct AgentConfig {
    // must be unique for each instance
    pub agent: String,
    pub cohort: String,
    // The size of internal buffer for candidates
    pub buffer_size: u32,
    pub timeout_ms: u32,
}

#[derive(Clone, Debug)]
pub enum TalosType {
    External,      // kafka listener and decision publisher is the external process
    InProcessMock, // kafka listener and decision publisher is out internal function
}

/// The agent interface exposed to the client
#[async_trait]
pub trait TalosAgent {
    async fn certify(&self, request: CertificationRequest) -> Result<CertificationResponse, AgentError>;
    async fn collect_metrics(&self) -> Option<MetricsReport>;
}

#[derive(Clone)]
pub enum TalosIntegrationType {
    /// The agent will publish certification requests to kafka
    Kafka,
    /// The agent will work in offline mode, simulating decisions
    InMemory,
}
