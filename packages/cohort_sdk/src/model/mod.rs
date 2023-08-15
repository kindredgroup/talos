pub mod callbacks;
pub mod internal;

use std::{collections::HashMap, fmt::Display};

use serde_json::Value;
use talos_agent::{
    agent::errors::{AgentError, AgentErrorKind},
    api::{AgentConfig, KafkaConfig, TalosType},
    messaging::api::Decision,
};
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct CandidateData {
    pub readset: Vec<String>,
    pub writeset: Vec<String>,
    pub statemap: Option<Vec<HashMap<String, Value>>>,
    // The "snapshot" is intentionally messing here. We will compute it ourselves before feeding this data to Talos
}

#[derive(Clone, Debug)]
pub struct Conflict {
    pub xid: String,
    pub version: u64,
    pub readvers: Vec<u64>,
}

#[derive(Clone)]
pub struct CertificationRequest {
    pub candidate: CandidateData,
    pub timeout_ms: u64,
}

#[derive(Clone)]
pub struct CertificationResponse {
    pub xid: String,
    pub decision: Decision,
    pub version: u64,
    pub safepoint: Option<u64>,
    pub conflict: Option<Conflict>,
    pub metadata: ResponseMetadata,
}

#[derive(Clone)]
pub struct ResponseMetadata {
    pub attempts: u64,
    pub duration_ms: u64,
}

#[derive(strum::Display)]
// this is napi friendly copy of talos_agent::agent::errors::AgentErrorKind
pub enum ClientErrorKind {
    Certification,
    CertificationTimeout,
    Messaging,
    Persistence,
    Internal,
    OutOfOrderCallbackFailed,
    OutOfOrderSnapshotTimeout,
}

pub struct ClientError {
    pub kind: ClientErrorKind,
    pub reason: String,
    pub cause: Option<String>,
}

#[derive(Clone)]
pub struct BackoffConfig {
    pub min_ms: u64,
    pub max_ms: u64,
}

impl BackoffConfig {
    pub fn new(min_ms: u64, max_ms: u64) -> Self {
        Self { min_ms, max_ms }
    }
}

#[derive(Clone)]
pub struct Config {
    //
    // cohort configs
    //
    pub backoff_on_conflict: BackoffConfig,
    pub retry_backoff: BackoffConfig,

    pub retry_attempts_max: u64,
    pub retry_oo_backoff: BackoffConfig,
    pub retry_oo_attempts_max: u64,

    pub snapshot_wait_timeout_ms: u64,

    //
    // agent config values
    //
    pub agent: String,
    pub cohort: String,
    // The size of internal buffer for candidates
    pub buffer_size: usize,
    pub timeout_ms: u64,

    //
    // Common to kafka configs values
    //
    pub brokers: String,
    pub topic: String,
    pub sasl_mechanisms: Option<String>,
    pub kafka_username: Option<String>,
    pub kafka_password: Option<String>,

    //
    // Kafka configs for Agent
    //
    // Must be unique for each agent instance. Can be the same as AgentConfig.agent_id
    pub agent_group_id: String,
    pub agent_fetch_wait_max_ms: u64,
    // The maximum time librdkafka may use to deliver a message (including retries)
    pub agent_message_timeout_ms: u64,
    // Controls how long to wait until message is successfully placed on the librdkafka producer queue  (including retries).
    pub agent_enqueue_timeout_ms: u64,
    // should be mapped to rdkafka::config::RDKafkaLogLevel
    pub agent_log_level: u64,

    //
    // Database config
    //
    pub db_pool_size: usize,
    pub db_user: String,
    pub db_password: String,
    pub db_host: String,
    pub db_port: String,
    pub db_database: String,
}
pub struct ReplicatorServices {
    pub replicator_handle: JoinHandle<Result<(), String>>,
    pub installer_handle: JoinHandle<Result<(), String>>,
}

impl From<Config> for AgentConfig {
    fn from(val: Config) -> Self {
        AgentConfig {
            agent: val.agent,
            cohort: val.cohort,
            buffer_size: val.buffer_size,
            timeout_ms: val.timeout_ms,
        }
    }
}

impl From<Config> for KafkaConfig {
    fn from(val: Config) -> Self {
        KafkaConfig {
            brokers: val.brokers,
            certification_topic: val.topic,
            sasl_mechanisms: val.sasl_mechanisms,
            username: val.kafka_username,
            password: val.kafka_password,
            group_id: val.agent_group_id,
            fetch_wait_max_ms: val.agent_fetch_wait_max_ms,
            message_timeout_ms: val.agent_message_timeout_ms,
            enqueue_timeout_ms: val.agent_enqueue_timeout_ms,
            log_level: KafkaConfig::map_log_level(val.agent_log_level),
            talos_type: TalosType::External,
        }
    }
}

impl From<AgentError> for ClientError {
    fn from(agent_error: AgentError) -> Self {
        let (kind, reason) = match agent_error.kind {
            AgentErrorKind::CertificationTimeout { xid, elapsed_ms } => (
                ClientErrorKind::CertificationTimeout,
                format!("Transaction {} timedout after {}ms", xid, elapsed_ms),
            ),

            AgentErrorKind::Messaging => (ClientErrorKind::Messaging, agent_error.reason),

            // error during cerification attempt, typically indicates early closure of internal buffers while some
            // transaction is not yet concluded
            AgentErrorKind::Certification { xid: _xid } => (ClientErrorKind::Internal, agent_error.reason),
        };

        Self {
            kind,
            reason,
            cause: agent_error.cause,
        }
    }
}

impl Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ClientError: [kind: {}, reason: {}, cause: {:?}]", self.kind, self.reason, self.cause)
    }
}
