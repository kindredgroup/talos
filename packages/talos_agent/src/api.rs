use crate::agent::errors::AgentError;
use crate::messaging::api::Decision;
use crate::metrics::model::MetricsReport;
use async_trait::async_trait;
use rdkafka::config::RDKafkaLogLevel;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Debug;
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
}

/// The data input from client to agent
#[derive(Clone, Debug)]
pub struct CertificationRequest {
    pub message_key: String,
    pub candidate: CandidateData,
    pub timeout: Option<Duration>,
}

/// The data output from agent to client
#[derive(Clone, Debug)]
pub struct CertificationResponse {
    pub xid: String,
    pub decision: Decision,
    pub version: u64,
    pub safepoint: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct AgentConfig {
    // must be unique for each instance
    pub agent: String,
    pub cohort: String,
    // The size of internal buffer for candidates
    pub buffer_size: usize,
    pub timout_ms: u64,
}

#[derive(Clone, Debug)]
pub enum TalosType {
    External,      // kafka listener and decision publisher is the external process
    InProcessMock, // kafka listener and decision publisher is out internal function
}

/// Kafka-related configuration
#[derive(Clone, Debug)]
pub struct KafkaConfig {
    pub brokers: String,
    // Must be unique for each agent instance. Can be the same as AgentConfig.agent_id
    pub group_id: String,
    pub certification_topic: String,
    pub fetch_wait_max_ms: u64,
    // The maximum time librdkafka may use to deliver a message (including retries)
    pub message_timeout_ms: u64,
    // Controls how long to wait until message is successfully placed on the librdkafka producer queue  (including retries).
    pub enqueue_timeout_ms: u64,
    pub log_level: RDKafkaLogLevel,
    pub talos_type: TalosType,
    // defaults to SCRAM-SHA-512
    pub sasl_mechanisms: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
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
