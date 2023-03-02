use crate::agentv2::decision_reader::DecisionReaderService;
use crate::agentv2::model::{CancelRequestChannelMessage, CertifyRequestChannelMessage};
use crate::agentv2::state_manager::StateManager;
use crate::api::{AgentConfig, CertificationRequest, CertificationResponse, KafkaConfig, TalosAgent};
use crate::messaging::api::{ConsumerType, DecisionMessage};
use crate::messaging::kafka::KafkaConsumer;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;
use tokio::time::error::Elapsed;
use tokio::time::timeout;

pub struct TalosAgentImplV2 {
    agent_config: AgentConfig,
    kafka_config: Option<KafkaConfig>,
    tx_certify: Sender<CertifyRequestChannelMessage>,
    tx_cancel: Sender<CancelRequestChannelMessage>,
}

impl TalosAgentImplV2 {
    pub fn new(
        agent_config: AgentConfig,
        kafka_config: Option<KafkaConfig>,
        tx_certify: Sender<CertifyRequestChannelMessage>,
        tx_cancel: Sender<CancelRequestChannelMessage>,
    ) -> TalosAgentImplV2 {
        TalosAgentImplV2 {
            agent_config,
            kafka_config,
            tx_certify,
            tx_cancel,
        }
    }
}

impl TalosAgentImplV2 {
    pub async fn start(
        &self,
        rx_certify: Receiver<CertifyRequestChannelMessage>,
        rx_cancel: Receiver<CancelRequestChannelMessage>,
        publish_times: Arc<Mutex<HashMap<String, u64>>>,
    ) {
        let agent_config = self.agent_config.clone();
        let kafka_config = self.kafka_config.clone().expect("Kafka config is required");
        let publish_times = Arc::clone(&publish_times);

        // channel to exchange decision messages between StateManager and DecisionReaderService
        let (tx_decision, rx_decision) = mpsc::channel::<DecisionMessage>(self.agent_config.buffer_size);

        // Start StateManager
        tokio::spawn(async move {
            StateManager::new(agent_config, kafka_config, publish_times)
                .run(rx_certify, rx_cancel, rx_decision)
                .await;
        });

        let (_, barrier) = self.start_reading_decisions(tx_decision);
        log::info!("Waiting until consumer is ready...");
        barrier.notified().await;
        log::info!("Consumer is ready.");
    }

    /// Spawn the task which hosts kafka Consumer connected with DecisionReaderService.
    /// Return task handle as well as the barrier which gets notified once consumer is ready.
    fn start_reading_decisions(&self, tx_decision: Sender<DecisionMessage>) -> (JoinHandle<()>, Arc<Notify>) {
        let agent = self.agent_config.agent.clone();
        let kafka_config = self.kafka_config.clone().expect("Kafka config is required");
        let barrier = Arc::new(Notify::new());
        let barrier_ref = Arc::clone(&barrier);
        let task_handle = tokio::spawn(async move {
            let consumer: Box<ConsumerType> = KafkaConsumer::new_subscribed(agent, &kafka_config)
                .map_err(|e| {
                    log::error!("{}", e);
                    e
                })
                .unwrap();
            log::info!("Created kafka consumer");

            barrier_ref.notify_one();

            let decision_reader = DecisionReaderService::new(consumer, tx_decision);
            decision_reader.run().await;
        });

        (task_handle, barrier)
    }
}

#[async_trait]
impl TalosAgent for TalosAgentImplV2 {
    async fn certify(&self, request: CertificationRequest) -> Result<CertificationResponse, String> {
        let (tx, mut rx) = mpsc::channel::<CertificationResponse>(1);

        let m = CertifyRequestChannelMessage::new(&request, &tx);
        let to_state_manager = self.tx_certify.clone();

        let max_wait: Duration = request.timeout.unwrap_or_else(|| Duration::from_millis(self.agent_config.timout_ms));

        let result: Result<Result<CertificationResponse, String>, Elapsed> = timeout(max_wait, async {
            match to_state_manager.send(m).await {
                Ok(()) => match rx.recv().await {
                    Some(mut response) => {
                        response.received_at = OffsetDateTime::now_utc().unix_timestamp_nanos() as u64;
                        Ok(response)
                    }
                    None => Err("No response received".to_string()),
                },
                Err(e) => Err(format!("No response received: {:?}", e.to_string())),
            }
        })
        .await;

        match result {
            Ok(rslt_certify) => rslt_certify,
            Err(_) => {
                let _ = self.tx_cancel.send(CancelRequestChannelMessage { request }).await;
                Err(format!("Unable to certify within {} millis", max_wait.as_millis()))
            }
        }
    }
}
