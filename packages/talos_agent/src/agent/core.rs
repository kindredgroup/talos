use crate::agent::decision_reader::DecisionReaderService;
use crate::agent::errors::AgentError;
use crate::agent::errors::AgentErrorKind::Certification;
use crate::agent::model::{CancelRequestChannelMessage, CertifyRequestChannelMessage};
use crate::agent::state_manager::StateManager;
use crate::api::{AgentConfig, CertificationRequest, CertificationResponse, KafkaConfig, TalosAgent};
use crate::messaging::api::{ConsumerType, DecisionMessage};
use crate::messaging::kafka::KafkaInitializer;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::error::Elapsed;
use tokio::time::timeout;

pub struct TalosAgentImpl {
    agent_config: AgentConfig,
    kafka_config: Option<KafkaConfig>,
    tx_certify: Sender<CertifyRequestChannelMessage>,
    tx_cancel: Sender<CancelRequestChannelMessage>,
}

impl TalosAgentImpl {
    pub fn new(
        agent_config: AgentConfig,
        kafka_config: Option<KafkaConfig>,
        tx_certify: Sender<CertifyRequestChannelMessage>,
        tx_cancel: Sender<CancelRequestChannelMessage>,
    ) -> TalosAgentImpl {
        TalosAgentImpl {
            agent_config,
            kafka_config,
            tx_certify,
            tx_cancel,
        }
    }
}

impl TalosAgentImpl {
    pub async fn start(
        &self,
        rx_certify: Receiver<CertifyRequestChannelMessage>,
        rx_cancel: Receiver<CancelRequestChannelMessage>,
        publish_times: Arc<Mutex<HashMap<String, u64>>>,
    ) -> Result<(), AgentError> {
        let agent_config = self.agent_config.clone();
        let kafka_config = self.kafka_config.clone().expect("Kafka config is required");
        let publish_times = Arc::clone(&publish_times);

        // channel to exchange decision messages between StateManager and DecisionReaderService
        let (tx_decision, rx_decision) = mpsc::channel::<DecisionMessage>(self.agent_config.buffer_size);

        let (publisher, consumer) = KafkaInitializer::connect(agent_config.agent.clone(), kafka_config.clone()).await?;

        log::info!("Publisher and Consumer are ready.");

        // Start StateManager
        tokio::spawn(async move {
            StateManager::new(agent_config, publish_times)
                .run(rx_certify, rx_cancel, rx_decision, publisher)
                .await;
        });

        let _ = self.start_reading_decisions(tx_decision, consumer);

        Ok(())
    }

    /// Spawn the task which hosts DecisionReaderService.
    /// Return task handle.
    fn start_reading_decisions(&self, tx_decision: Sender<DecisionMessage>, consumer: Arc<Box<ConsumerType>>) -> JoinHandle<()> {
        let consumer_ref = Arc::clone(&consumer);
        tokio::spawn(async move {
            let decision_reader = DecisionReaderService::new(consumer_ref, tx_decision);
            decision_reader.run().await;
        })
    }
}

#[async_trait]
impl TalosAgent for TalosAgentImpl {
    async fn certify(&self, request: CertificationRequest) -> Result<CertificationResponse, AgentError> {
        let (tx, mut rx) = mpsc::channel::<CertificationResponse>(1);

        let m = CertifyRequestChannelMessage::new(&request, &tx);
        let to_state_manager = self.tx_certify.clone();

        let max_wait: Duration = request.timeout.unwrap_or_else(|| Duration::from_millis(self.agent_config.timout_ms));

        let result: Result<Result<CertificationResponse, AgentError>, Elapsed> = timeout(max_wait, async {
            match to_state_manager.send(m).await {
                Ok(()) => match rx.recv().await {
                    Some(mut response) => {
                        response.received_at = OffsetDateTime::now_utc().unix_timestamp_nanos() as u64;
                        Ok(response)
                    }
                    None => Err(AgentError {
                        kind: Certification {
                            xid: request.candidate.xid.clone(),
                        },
                        reason: "No response from state manager".to_string(),
                        cause: None,
                    }),
                },
                Err(e) => Err(e.into()),
            }
        })
        .await;

        let xid = request.candidate.xid.clone();
        match result {
            Ok(rslt_certify) => rslt_certify,
            Err(_) => {
                let _ = self.tx_cancel.send(CancelRequestChannelMessage { request }).await;
                Err(AgentError::new_certify_timout(xid, max_wait.as_millis()))
            }
        }
    }
}
