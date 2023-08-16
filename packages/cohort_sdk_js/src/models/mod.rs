use std::collections::HashMap;
use napi::bindgen_prelude::FromNapiValue;
use napi::{JsObject, JsUnknown};
use napi::sys::{napi_env, napi_value};
// $coverage:ignore-start
use napi_derive::napi;
use cohort_sdk::model::Config;

// unsafe impl Send for JsConfig {}
// unsafe impl Sync for JsConfig {}
#[napi(object)]
pub struct JsConfig {
    //
    // cohort configs
    //
    pub retry_attempts_max: u32,
    pub retry_backoff_max_ms: u32,
    pub retry_oo_backoff_max_ms: u32,
    pub retry_oo_attempts_max: u32,

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
    // Kafka configs for Replicator
    //
    pub replicator_client_id: String,
    pub replicator_group_id: String,
    pub producer_config_overrides: HashMap<String, String>,
    pub consumer_config_overrides: HashMap<String, String>,

    //
    // Suffix config values
    //
    /// Initial capacity of the suffix
    pub suffix_size_max: u32,
    /// - The suffix prune threshold from when we start checking if the suffix
    /// should prune.
    /// - Set to None if pruning is not required.
    /// - Defaults to None.
    pub suffix_prune_at_size: Option<u32>,
    /// Minimum size of suffix after prune.
    /// - Defaults to None.
    pub suffix_size_min: Option<u32>,

    //
    // Replicator config values
    //
    pub replicator_buffer_size: u32,

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


impl Into<Config> for JsConfig {
    fn into(self) -> Config {
        Config {
            //
            // cohort configs
            //
            retry_attempts_max: 10,
            retry_backoff_max_ms: 1500,
            retry_oo_backoff_max_ms: 1000,
            retry_oo_attempts_max: 10,

            //
            // agent config values
            //
            agent: "cohort-banking".into(),
            cohort: "cohort-banking".into(),
            // The size of internal buffer for candidates
            buffer_size: 10_000_000,
            timeout_ms: 600_000,

            //
            // Common to kafka configs values
            //
            brokers: "127.0.0.1:9092".into(),
            topic: "dev.ksp.certification".into(),
            sasl_mechanisms: None,
            kafka_username: None,
            kafka_password: None,

            //
            // Kafka configs for Agent
            //
            // Must be unique for each agent instance. Can be the same as AgentConfig.agent_id
            agent_group_id: "cohort-banking".into(),
            agent_fetch_wait_max_ms: 6000,
            // The maximum time librdkafka may use to deliver a message (including retries)
            agent_message_timeout_ms: 15000,
            // Controls how long to wait until message is successfully placed on the librdkafka producer queue  (including retries).
            agent_enqueue_timeout_ms: 10,
            // should be mapped to rdkafka::config::RDKafkaLogLevel
            agent_log_level: 6,

            //
            // Kafka configs for Replicator
            //
            replicator_client_id: "cohort-banking".into(),
            replicator_group_id: "cohort-banking-replicator".into(),
            producer_config_overrides: HashMap::new(),
            consumer_config_overrides: HashMap::new(),

            //
            // Suffix config values
            //
            /// Initial capacity of the suffix
            // suffix_size_max: 500_000,
            suffix_size_max: 10,
            /// - The suffix prune threshold from when we start checking if the suffix
            /// should prune.
            /// - Set to None if pruning is not required.
            /// - Defaults to None.
            // suffix_prune_at_size: Some(300_000),
            suffix_prune_at_size: Some(2000),
            /// Minimum size of suffix after prune.
            /// - Defaults to None.
            // suffix_size_min: Some(100_000),
            suffix_size_min: None,

            //
            // Replicator config values
            //
            replicator_buffer_size: 100_000,

            //
            // Database config
            //
            db_pool_size: 200,
            db_user: "postgres".into(),
            db_password: "admin".into(),
            db_host: "127.0.0.1".into(),
            db_port: "5432".into(),
            db_database: "talos-sample-cohort-dev".into(),
        }
    }
    // fn into(self) -> Config {
    //     Config {
    //         //
    //         // cohort configs
    //         //
    //         retry_attempts_max: self.retry_attempts_max,
    //         retry_backoff_max_ms: self.retry_backoff_max_ms,
    //         retry_oo_backoff_max_ms: self.retry_oo_backoff_max_ms,
    //         retry_oo_attempts_max: self.retry_oo_attempts_max,
    //
    //         //
    //         // agent config values
    //         //
    //         agent: self.agent,
    //         cohort: self.cohort,
    //         // The size of internal buffer for candidates
    //         buffer_size: self.buffer_size,
    //         timeout_ms: self.timeout_ms,
    //
    //         //
    //         // Common to kafka configs values
    //         //
    //         brokers: self.brokers,
    //         topic: self.topic,
    //         sasl_mechanisms: self.sasl_mechanisms,
    //         kafka_username: self.kafka_username,
    //         kafka_password: self.kafka_password,
    //
    //         //
    //         // Kafka configs for Agent
    //         //
    //         // Must be unique for each agent instance. Can be the same as AgentConfig.agent_id
    //         agent_group_id: self.agent_group_id,
    //         agent_fetch_wait_max_ms: self.agent_fetch_wait_max_ms,
    //         // The maximum time librdkafka may use to deliver a message (including retries)
    //         agent_message_timeout_ms: self.agent_message_timeout_ms,
    //         // Controls how long to wait until message is successfully placed on the librdkafka producer queue  (including retries).
    //         agent_enqueue_timeout_ms: self.agent_enqueue_timeout_ms,
    //         // should be mapped to rdkafka::config::RDKafkaLogLevel
    //         agent_log_level: self.agent_log_level,
    //
    //         //
    //         // Kafka configs for Replicator
    //         //
    //         replicator_client_id: self.replicator_client_id,
    //         replicator_group_id: self.replicator_group_id,
    //         producer_config_overrides: self.producer_config_overrides,
    //         consumer_config_overrides: self.consumer_config_overrides,
    //
    //         //
    //         // Suffix config values
    //         //
    //         /// Initial capacity of the suffix
    //         // suffix_size_max: 500_000,
    //         suffix_size_max: self.suffix_size_max,
    //         /// - The suffix prune threshold from when we start checking if the suffix
    //         /// should prune.
    //         /// - Set to None if pruning is not required.
    //         /// - Defaults to None.
    //         // suffix_prune_at_size: Some(300_000),
    //         suffix_prune_at_size: self.suffix_prune_at_size,
    //         /// Minimum size of suffix after prune.
    //         /// - Defaults to None.
    //         // suffix_size_min: Some(100_000),
    //         suffix_size_min: self.suffix_size_min,
    //
    //         //
    //         // Replicator config values
    //         //
    //         replicator_buffer_size: self.replicator_buffer_size,
    //
    //         //
    //         // Database config
    //         //
    //         db_pool_size: self.db_pool_size,
    //         db_user: self.db_user,
    //         db_password: self.db_password,
    //         db_host: self.db_host,
    //         db_port: self.db_port,
    //         db_database: self.db_database,
    //     }
    // }
}
