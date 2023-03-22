use crate::agent::core::TalosAgentImpl;
use crate::agent::errors::AgentError;
use crate::agent::model::{CancelRequestChannelMessage, CertifyRequestChannelMessage};
use crate::messaging::api::Decision;
use crate::metrics;
use crate::metrics::client::MetricsClient;
use crate::metrics::model::{MetricsReport, Signal};
use async_trait::async_trait;
use rdkafka::config::RDKafkaLogLevel;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

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
    pub timeout: Option<Duration>,
}

/// The data output from agent to client
#[derive(Clone, Debug)]
pub struct CertificationResponse {
    pub xid: String,
    pub decision: Decision,
}

#[derive(Clone)]
pub struct AgentConfig {
    // must be unique for each instance
    pub agent: String,
    pub cohort: String,
    // The size of internal buffer for candidates
    pub buffer_size: usize,
    pub timout_ms: u64,
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
    pub log_level: RDKafkaLogLevel,
    pub talos_type: TalosType,
    // defaults to SCRAM-SHA-512
    pub sasl_mechanisms: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
}

/// The agent interface exposed to the client
#[async_trait]
pub trait TalosAgent {
    async fn certify(&self, request: CertificationRequest) -> Result<CertificationResponse, AgentError>;
    fn collect_metrics(&self) -> Option<MetricsReport>;
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
    add_metrics: bool,
}

impl TalosAgentBuilder {
    pub fn new(config: AgentConfig) -> TalosAgentBuilder {
        Self {
            config,
            kafka_config: None,
            add_metrics: false,
        }
    }

    /// When agent is built it will be connected to external kafka broker.
    pub fn with_kafka(&mut self, config: KafkaConfig) -> &mut Self {
        self.kafka_config = Some(config);
        self
    }

    pub fn with_metrics(&mut self) -> &mut Self {
        self.add_metrics = true;
        self
    }

    /// Build agent instance implemented using actor model.
    pub async fn build(&self) -> Result<Box<TalosAgentType>, AgentError> {
        let (tx_certify, rx_certify) = mpsc::channel::<CertifyRequestChannelMessage>(self.config.buffer_size);
        let (tx_cancel, rx_cancel) = mpsc::channel::<CancelRequestChannelMessage>(self.config.buffer_size);

        if self.add_metrics {
            let metrics = metrics::core::Metrics::new();

            let (tx, rx) = mpsc::channel::<Signal>(100_000);
            metrics.run(rx);

            let metrics_client = Box::new(MetricsClient { tx_destination: tx.clone() });

            let agent = TalosAgentImpl::new(
                self.config.clone(),
                self.kafka_config.clone(),
                tx_certify,
                tx_cancel,
                Some(metrics),
                Arc::new(Some(metrics_client)),
            );
            agent.start(rx_certify, rx_cancel).await?;

            Ok(Box::new(agent))
        } else {
            let agent = TalosAgentImpl::new(self.config.clone(), self.kafka_config.clone(), tx_certify, tx_cancel, None, Arc::new(None));
            agent.start(rx_certify, rx_cancel).await?;

            Ok(Box::new(agent))
        }
    }
}
