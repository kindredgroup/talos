// $coverage:ignore-start
use cohort_sdk::model::{BackoffConfig, Config};
use napi_derive::napi;

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
    pub agent_fetch_wait_max_ms: u32,
    // The maximum time librdkafka may use to deliver a message (including retries)
    pub agent_message_timeout_ms: u32,
    // Controls how long to wait until message is successfully placed on the librdkafka producer queue  (including retries).
    pub agent_enqueue_timeout_ms: u32,
    // should be mapped to rdkafka::config::RDKafkaLogLevel
    pub agent_log_level: u32,

    //
    // Database config
    //
    pub db_pool_size: u32,
    pub db_user: String,
    pub db_password: String,
    pub db_host: String,
    pub db_port: String,
    pub db_database: String,
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

            //
            // Common to kafka configs values
            //
            brokers: val.brokers,
            topic: val.topic,
            sasl_mechanisms: val.sasl_mechanisms,
            kafka_username: val.kafka_username,
            kafka_password: val.kafka_password,

            //
            // Kafka configs for Agent
            //
            // Must be unique for each agent instance. Can be the same as AgentConfig.agent_id
            agent_group_id: val.agent_group_id,
            agent_fetch_wait_max_ms: val.agent_fetch_wait_max_ms,
            // The maximum time librdkafka may use to deliver a message (including retries)
            agent_message_timeout_ms: val.agent_message_timeout_ms,
            // Controls how long to wait until message is successfully placed on the librdkafka producer queue  (including retries).
            agent_enqueue_timeout_ms: val.agent_enqueue_timeout_ms,
            // should be mapped to rdkafka::config::RDKafkaLogLevel
            agent_log_level: val.agent_log_level,

            //
            // Database config
            //
            db_pool_size: val.db_pool_size,
            db_user: val.db_user,
            db_password: val.db_password,
            db_host: val.db_host,
            db_port: val.db_port,
            db_database: val.db_database,
        }
    }
}
