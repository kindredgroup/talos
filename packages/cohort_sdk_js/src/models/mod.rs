use cohort_sdk::model::{BackoffConfig, CohortOtelConfig};
use std::collections::HashMap;
use talos_agent::messaging::api::Decision;
use talos_rdkafka_utils::kafka_config::KafkaConfig;

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

#[derive(Clone)]
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

#[napi(string_enum)]
pub enum JsDecision {
    Committed,
    Aborted,
}

impl From<Decision> for JsDecision {
    fn from(value: Decision) -> Self {
        match value {
            Decision::Committed => JsDecision::Committed,
            Decision::Aborted => JsDecision::Aborted,
        }
    }
}

#[napi(object)]
#[derive(Debug)]
pub struct JsCohortOtelConfig {
    pub name: String,
    pub enabled: bool,
    pub grpc_endpoint: Option<String>,
}

impl From<JsCohortOtelConfig> for CohortOtelConfig {
    fn from(val: JsCohortOtelConfig) -> Self {
        CohortOtelConfig {
            name: val.name,
            enabled: val.enabled,
            grpc_endpoint: val.grpc_endpoint,
        }
    }
}
