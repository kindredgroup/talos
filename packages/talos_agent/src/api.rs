use crate::agent::TalosAgentImpl;
use crate::api::TalosIntegrationType::{InMemory, Kafka};
use crate::messaging::api::PublisherType;
use crate::messaging::kafka::KafkaPublisher;
use crate::messaging::mock::MockPublisher;
use async_trait::async_trait;
use rdkafka::config::RDKafkaLogLevel;

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
    pub polled_total: i32,
    pub polled_empty: i32,
    pub polled_others: i32,
}

#[derive(Clone)]
pub struct AgentConfig {
    pub agent_name: String,
    pub cohort_name: String,
}

/// Kafka-related configuration
#[derive(Clone)]
pub struct KafkaConfig {
    pub brokers: String,
    pub certification_topic: String,
    // The maximum time librdkafka may use to deliver a message (including retries)
    pub message_timeout_ms: u64,
    // Controls how long to wait until message is successfully placed on the librdkafka producer queue  (including retries).
    pub enqueue_timeout_ms: u64,
    // Group session keepalive heartbeat interval
    // pub heartbeat_interval_ms: u64,
    pub log_level: RDKafkaLogLevel,
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

    pub fn build_sc(&self) -> Result<Box<TalosAgentType>, String> {
        let publisher: Box<PublisherType> = match self.integration_type {
            Kafka => {
                let config = &self.kafka_config.clone().expect("Kafka configuration is required");
                let kafka_publisher = KafkaPublisher::new(config);
                Box::new(kafka_publisher)
            }
            _ => Box::new(MockPublisher),
        };

        let agent = TalosAgentImpl::new(self.config.clone(), self.kafka_config.clone(), publisher);
        Ok(Box::new(agent))
    }
}
