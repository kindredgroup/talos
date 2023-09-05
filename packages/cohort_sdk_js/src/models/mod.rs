use std::collections::HashMap;

// $coverage:ignore-start
use cohort_sdk::model::{BackoffConfig, Config};
use napi_derive::napi;
use talos_rdkafka_utils::kafka_config::KafkaConfig;

#[napi(object)]
pub struct JsBackoffConfig {
    pub min_ms: u32,
    pub max_ms: u32,
}

impl From<JsBackoffConfig> for BackoffConfig {
    fn from(val: JsBackoffConfig) -> Self {
        BackoffConfig {
            min_ms: val.min_ms,
            max_ms: val.max_ms,
        }
    }
}

#[napi(object)]
pub struct JsKafkaConfig {
    pub brokers: Vec<String>,
    pub topic: String,
    pub client_id: String,
    // Must be unique for each agent instance. Can be the same as AgentConfig.agent_id
    pub group_id: String,
    pub username: String,
    pub password: String,
    // The maximum time librdkafka may use to deliver a message (including retries)
    pub producer_config_overrides: HashMap<String, String>,
    pub consumer_config_overrides: HashMap<String, String>,
    // consumer_config_overrides: HashMap::new(),
    pub producer_send_timeout_ms: Option<u32>,
    pub log_level: Option<String>,
}

impl From<JsKafkaConfig> for KafkaConfig {
    fn from(val: JsKafkaConfig) -> Self {
        KafkaConfig {
            brokers: val.brokers,
            topic: val.topic,
            client_id: val.client_id,
            // Must be unique for each agent instance. Can be the same as AgentConfig.agent_id
            group_id: val.group_id,
            username: val.username,
            password: val.password,
            // The maximum time librdkafka may use to deliver a message (including retries)
            producer_config_overrides: val.producer_config_overrides,
            consumer_config_overrides: val.consumer_config_overrides,
            // consumer_config_overrides: HashMap::new(),
            producer_send_timeout_ms: val.producer_send_timeout_ms,
            log_level: val.log_level,
        }
    }
}

// unsafe impl Send for JsConfig {}
// unsafe impl Sync for JsConfig {}
#[napi(object)]
pub struct JsConfig {
    // cohort configs
    //
    pub backoff_on_conflict: JsBackoffConfig,
    pub retry_backoff: JsBackoffConfig,

    pub retry_attempts_max: u32,
    pub retry_oo_backoff: JsBackoffConfig,
    pub retry_oo_attempts_max: u32,

    pub snapshot_wait_timeout_ms: u32,

    //
    // agent config values
    //
    pub agent: String,
    pub cohort: String,
    // The size of internal buffer for candidates
    pub buffer_size: u32,
    pub timeout_ms: u32,

    pub kafka: JsKafkaConfig,
}

impl From<JsConfig> for Config {
    fn from(val: JsConfig) -> Self {
        Config {
            //
            // cohort configs
            //
            retry_attempts_max: val.retry_attempts_max,
            retry_backoff: val.retry_backoff.into(),
            backoff_on_conflict: val.backoff_on_conflict.into(),
            retry_oo_backoff: val.retry_oo_backoff.into(),
            retry_oo_attempts_max: val.retry_oo_attempts_max,
            snapshot_wait_timeout_ms: val.snapshot_wait_timeout_ms,

            //
            // agent config values
            //
            agent: val.agent,
            cohort: val.cohort,
            // The size of internal buffer for candidates
            buffer_size: val.buffer_size,
            timeout_ms: val.timeout_ms,

            kafka: val.kafka.into(),
        }
    }
}
