pub mod callbacks;
pub mod internal;

use std::{collections::HashMap, fmt::Display};

use serde_json::Value;
use talos_agent::{
    agent::errors::{AgentError, AgentErrorKind},
    api::AgentConfig,
    messaging::api::Decision,
};
use talos_rdkafka_utils::kafka_config::KafkaConfig;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct CandidateData {
    pub readset: Vec<String>,
    pub writeset: Vec<String>,
    pub statemap: Option<Vec<HashMap<String, Value>>>,
    // The "snapshot" is intentionally messing here. We will compute it ourselves before feeding this data to Talos
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
    pub conflict: Option<u64>,
    pub metadata: ResponseMetadata,
}

#[derive(Clone)]
pub struct ResponseMetadata {
    pub attempts: u32,
    pub duration_ms: u64,
}

#[derive(strum::Display)]
// this is napi friendly copy of talos_agent::agent::errors::AgentErrorKind
pub enum ClientErrorKind {
    Certification,
    CertificationTimeout,
    ClientAborted,
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
    pub min_ms: u32,
    pub max_ms: u32,
}

impl BackoffConfig {
    pub fn new(min_ms: u32, max_ms: u32) -> Self {
        Self { min_ms, max_ms }
    }
}

#[derive(Clone)]
pub struct Config {
    //
    // Cohort configs
    //
    // Backoff setting before re-polling after Talos returned abort caused by conflict
    pub backoff_on_conflict: BackoffConfig,
    // Backoff setting before re-trying to send request to Talos
    pub retry_backoff: BackoffConfig,

    pub retry_attempts_max: u32,
    // Backoff setting before re-trying DB install operations during when handling out of order installs
    pub retry_oo_backoff: BackoffConfig,
    pub retry_oo_attempts_max: u32,

    pub snapshot_wait_timeout_ms: u32,

    //
    // Agent config values
    //
    pub agent: String,
    pub cohort: String,
    // The size of internal buffer for candidates
    pub buffer_size: u32,
    pub timeout_ms: u32,

    //
    // Kafka configs for Agent
    //
    pub kafka: KafkaConfig,
}

impl Config {
    pub fn create(agent: String, cohort: String, kafka_config: KafkaConfig) -> Self {
        Self {
            backoff_on_conflict: BackoffConfig { min_ms: 1, max_ms: 1500 },
            retry_backoff: BackoffConfig { min_ms: 20, max_ms: 1500 },
            retry_attempts_max: 10,
            retry_oo_backoff: BackoffConfig { min_ms: 20, max_ms: 1500 },
            retry_oo_attempts_max: 10,
            snapshot_wait_timeout_ms: 10_000,
            agent,
            cohort,
            buffer_size: 100_000,
            timeout_ms: 30_000,

            // Kafka
            kafka: kafka_config,
        }
    }
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
