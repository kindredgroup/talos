///
/// Data structures and interfaces exposed to agent client
///
use crate::agent::{TalosAgentImpl};
use crate::api::TalosIntegrationType::{InMemory, Kafka};
use crate::messaging::api::Publisher;
use crate::messaging::kafka::KafkaPublisher;
use crate::messaging::mock::MockPublisher;

/// The main candidate payload
#[derive(Clone, Debug)]
pub struct CandidateData {
    pub xid: String,
    pub readset: Vec<String>,
    pub readvers: Vec<u64>,
    pub snapshot: u64,
    pub writeset: Vec<u64>,
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
    pub partition: i32,
    pub offset: i64,
    pub is_accepted: bool,
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
    pub message_timeout_ms: u64,
    pub certification_topic: String,
}

/// The agent interface exposed to the client
pub trait TalosAgent {
    fn certify(&self, request: CertificationRequest) -> Result<CertificationResponse, String>;
}

pub enum TalosIntegrationType {
    /// The agent will publish certification requests to kafka
    Kafka,
    /// The agent will work in offline mode, simulating decisions
    InMemory
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
        self.kafka_config = Some(config.clone());
        self.integration_type = Kafka;
        self
    }

    /// Build an instance of agent.
    pub fn build(&self) -> Box<dyn TalosAgent> {
        let publisher: Box<dyn Publisher> = match self.integration_type {
            Kafka => {
                let config = &self.kafka_config.clone().expect("Kafka configuration is required");
                let kafka_publisher = KafkaPublisher::new(config);
                Box::new(kafka_publisher)
            },
            _ => {
                Box::new(MockPublisher)
            },
        };

        Box::new(TalosAgentImpl {
            config: self.config.clone(),
            publisher
        })
    }
}

