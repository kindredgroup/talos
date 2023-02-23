use crate::agent::TalosAgentImpl;
use crate::agentv2::agent_v2::TalosAgentImplV2;
use crate::agentv2::model::{CancelRequestChannelMessage, CertifyRequestChannelMessage};
use crate::api::TalosIntegrationType::{InMemory, Kafka};
use crate::messaging::api::PublisherType;
use crate::messaging::kafka::KafkaPublisher;
use crate::messaging::mock::MockPublisher;
use async_trait::async_trait;
use rdkafka::config::RDKafkaLogLevel;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub const TRACK_PUBLISH_LATENCY: bool = false;

///
/// Data structures and interfaces exposed to agent client
///

/// The main candidate payload
#[derive(Clone, Debug)]
pub struct CandidateData {
    pub xid: String,
    pub readset: Vec<String>,
    pub readvers: Vec<u64>,
    pub snapshot: u64,
    pub writeset: Vec<String>,
}

/// The data input from client to agent
#[derive(Clone, Debug)]
pub struct CertificationRequest {
    pub message_key: String,
    pub candidate: CandidateData,
}

/// The data output from agent to client
#[derive(Clone, Debug)]
pub struct CertificationResponse {
    pub xid: String,
    pub is_accepted: bool,
    pub send_started_at: u64,
    pub received_at: u64,
    pub decided_at: u64,
    pub decision_buffered_at: u64,
}

#[derive(Clone)]
pub struct AgentConfig {
    // must be unique for each instance
    pub agent: String,
    pub cohort: String,
    // The size of internal buffer for candidates
    pub buffer_size: usize,
}

#[derive(Clone)]
pub enum TalosType {
    External,      // kafka listener and decision publisher is the external process
    InProcessMock, // kafka listener and decision publisher is out internal function
}

/// Kafka-related configuration
#[derive(Clone)]
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
    // Group session keepalive heartbeat interval
    // pub heartbeat_interval_ms: u64,
    pub log_level: RDKafkaLogLevel,
    pub talos_type: TalosType,
}

/// The agent interface exposed to the client
#[async_trait]
pub trait TalosAgent {
    async fn certify(&self, request: CertificationRequest) -> Result<CertificationResponse, String>;
}

pub type TalosAgentType = dyn TalosAgent + Send + Sync;

#[derive(Clone)]
pub enum TalosIntegrationType {
    /// The agent will publish certification requests to kafka
    Kafka,
    /// The agent will work in offline mode, simulating decisions
    InMemory,
}

/// Builds the agent instance, my default agent will be build using in-memory integration type.
/// The integration type can be changed via "with_kafka" setting.
pub struct TalosAgentBuilder {
    config: AgentConfig,
    kafka_config: Option<KafkaConfig>,
    integration_type: TalosIntegrationType,
}

impl TalosAgentBuilder {
    pub fn new(config: AgentConfig) -> TalosAgentBuilder {
        Self {
            config,
            kafka_config: None,
            integration_type: InMemory,
        }
    }

    /// When agent is built it will be connected to external kafka broker.
    pub fn with_kafka(&mut self, config: KafkaConfig) -> &mut Self {
        self.kafka_config = Some(config);
        self.integration_type = Kafka;
        self
    }

    /// Build agent instance implemented using shared state between threads.
    pub fn build(&self) -> Result<Box<TalosAgentType>, String> {
        let publisher: Box<PublisherType> = match self.integration_type {
            Kafka => {
                let config = &self.kafka_config.clone().expect("Kafka configuration is required");
                let kafka_publisher = KafkaPublisher::new(self.config.agent.clone(), config);
                Box::new(kafka_publisher)
            }
            _ => Box::new(MockPublisher {}),
        };

        let agent = TalosAgentImpl::new(self.config.clone(), self.kafka_config.clone(), publisher);
        agent
            .start(&self.integration_type)
            .unwrap_or_else(|e| panic!("{}", format!("Unable to start agent {}", e)));

        Ok(Box::new(agent))
    }

    /// Build agent instance implemented using actor model.
    pub async fn build_v2(&self, publish_times: &Arc<Mutex<HashMap<String, u64>>>) -> Result<Box<TalosAgentType>, String> {
        let agent = TalosAgentImplV2::new(
            self.config.clone(),
            self.kafka_config.clone(),
            &self.integration_type,
            tokio::sync::mpsc::channel::<CertifyRequestChannelMessage>(self.config.buffer_size),
            tokio::sync::mpsc::channel::<CancelRequestChannelMessage>(self.config.buffer_size),
            publish_times,
        )
        .await
        .map_err(|e| {
            log::error!("Unable to create agent instance: {}", e);
            e
        })
        .unwrap();

        Ok(Box::new(agent))
    }
}
