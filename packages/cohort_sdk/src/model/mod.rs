pub mod callbacks;
pub mod internal;

use std::{collections::HashMap, fmt::Display};

use serde_json::Value;
use talos_agent::{
    agent::errors::{AgentError, AgentErrorKind},
    api::{AgentConfig, KafkaConfig, TalosType},
    messaging::api::Decision,
};
use talos_certifier_adapters::kafka::config::KafkaConfig as TalosKafkaConfig;
use talos_suffix::core::SuffixConfig;
use tokio::task::JoinHandle;

// #[napi]
#[derive(Clone)]
pub struct CandidateData {
    pub readset: Vec<String>,
    pub writeset: Vec<String>,
    pub statemap: Option<Vec<HashMap<String, Value>>>,
    // The "snapshot" is intentionally messing here. We will compute it ourselves before feeding this data to Talos
}

// #[napi]
#[derive(Clone)]
pub struct CertificationRequest {
    pub candidate: CandidateData,
    pub timeout_ms: u64,
}

// #[napi]
pub struct CertificationResponse {
    pub xid: String,
    pub decision: Decision,
    pub version: u64,
    pub safepoint: Option<u64>,
    pub metadata: ResponseMetadata,
}

pub struct ResponseMetadata {
    pub attempts: u64,
    pub duration_ms: u64,
}

#[derive(strum::Display)]
// #[napi]
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

// #[napi]
pub struct ClientError {
    pub kind: ClientErrorKind,
    pub reason: String,
    pub cause: Option<String>,
}

#[derive(Clone)]
// #[napi]
pub struct Config {
    //
    // cohort configs
    //
    pub retry_attempts_max: u64,
    pub retry_backoff_max_ms: u64,
    pub retry_oo_backoff_max_ms: u64,
    pub retry_oo_attempts_max: u64,

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
    // Kafka configs for Replicator
    //
    pub replicator_client_id: String,
    pub replicator_group_id: String,
    pub producer_config_overrides: HashMap<&'static str, &'static str>,
    pub consumer_config_overrides: HashMap<&'static str, &'static str>,

    //
    // Suffix config values
    //
    /// Initial capacity of the suffix
    pub suffix_size_max: usize,
    /// - The suffix prune threshold from when we start checking if the suffix
    /// should prune.
    /// - Set to None if pruning is not required.
    /// - Defaults to None.
    pub suffix_prune_at_size: Option<usize>,
    /// Minimum size of suffix after prune.
    /// - Defaults to None.
    pub suffix_size_min: Option<usize>,

    //
    // Replicator config values
    //
    pub replicator_buffer_size: usize,

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

impl From<Config> for SuffixConfig {
    fn from(val: Config) -> Self {
        SuffixConfig {
            capacity: val.suffix_size_max,
            prune_start_threshold: val.suffix_prune_at_size,
            min_size_after_prune: val.suffix_size_min,
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

impl From<Config> for TalosKafkaConfig {
    fn from(val: Config) -> Self {
        TalosKafkaConfig {
            brokers: val.brokers.split(',').map(|i| i.to_string()).collect(),
            topic: val.topic,
            // TODO: not sure how napi will handle Option<> fields, if it can process them then we dont need to use this mapping.
            username: val.kafka_username.unwrap_or_else(|| "".into()),
            // TODO: not sure how napi will handle Option<> fields, if it can process them then we dont need to use this mapping.
            password: val.kafka_password.unwrap_or_else(|| "".into()),
            client_id: val.replicator_client_id,
            group_id: val.replicator_group_id,
            producer_config_overrides: val.producer_config_overrides,
            consumer_config_overrides: val.consumer_config_overrides,
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
