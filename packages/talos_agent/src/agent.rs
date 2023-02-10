use async_trait::async_trait;
use log::debug;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

use crate::api::{AgentConfig, CertificationRequest, CertificationResponse, KafkaConfig, TalosAgent, TalosIntegrationType};
use crate::messaging::api::{CandidateMessage, ConsumerType, Decision, DecisionMessage, PublisherType};
use crate::messaging::kafka::KafkaConsumer;
use crate::messaging::mock::MockConsumer;

///
/// Agent implementation which uses single consumer task. Once decision is received, it will lookup for
/// pending in-flight transaction request by xid and notify it.
///
pub struct TalosAgentImpl {
    pub config: AgentConfig,
    pub kafka_config: Option<KafkaConfig>,
    pub publisher: Box<PublisherType>,
    // Key is xid, value is "shelf" where we keep notifier handle and exchanged value.
    pub in_flight: Arc<Mutex<HashMap<String, Arc<InFlight>>>>,
}

pub struct InFlight {
    pub xid: String,
    // the exchanged value
    pub decision: Mutex<Option<DecisionMessage>>,
    pub monitor: Notify,
}

impl TalosAgentImpl {
    /// Makes an instance of agent which is connected to kafka broker and already listening for incoming messages
    pub fn new(config: AgentConfig, kafka_config: Option<KafkaConfig>, publisher: Box<PublisherType>) -> Self {
        // todo:
        //      The 'kafka_config' is needed to create a consumer from within task. Given that consumer is trait,
        //      the outside builder should be creating Kafka or Mock consumer. Maybe it is better if task itself is
        //      passed created by builder, as I am not yet sure how to move consumer into the task.
        TalosAgentImpl {
            config,
            kafka_config,
            publisher,
            in_flight: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn create_kafka_consumer(agent_id: String, config: &KafkaConfig) -> Result<Box<ConsumerType>, String> {
        let kc = KafkaConsumer::new(agent_id, config);
        match kc.subscribe() {
            Ok(()) => Ok(Box::new(kc)),
            Err(e) => Err(e),
        }
    }

    fn create_mock_consumer(_config: &KafkaConfig) -> Result<Box<ConsumerType>, String> {
        Ok(Box::new(MockConsumer {}))
    }

    /// Starts listener task which reads Talos decisions from the topic
    pub fn start(&self, int_type: &TalosIntegrationType) -> Result<(), String> {
        let agent_id = self.config.agent_id.clone();
        let config = self.kafka_config.clone().expect("Kafka configuration is required");
        let in_flight = Arc::clone(&self.in_flight);

        let it = int_type.clone();
        tokio::spawn(async move {
            let consumer: Box<ConsumerType> = match it {
                TalosIntegrationType::Kafka => Self::create_kafka_consumer(agent_id, &config),
                TalosIntegrationType::InMemory => Self::create_mock_consumer(&config),
            }
            .unwrap();

            loop {
                match consumer.receive_message().await {
                    Some(Ok(received_message)) => {
                        let state = in_flight.lock().unwrap();

                        // check if candidate was posted by this agent
                        match state.get(received_message.xid.as_str()) {
                            None => {
                                debug!("receive_message(): skip xid: {}", received_message.xid);
                                continue;
                            }
                            Some(pending) => {
                                let mut decision = pending.decision.lock().unwrap();
                                *decision = Some(received_message);
                                pending.monitor.notify_one();
                            }
                        }
                    }
                    _ => continue,
                }
            }
        });

        Ok(())
    }
}

#[async_trait]
impl TalosAgent for TalosAgentImpl {
    /// Certifies transaction represented by given request object. Caller of this method should '.await' for
    /// the response.
    async fn certify(&self, request: CertificationRequest) -> Result<CertificationResponse, String> {
        let in_flight = Arc::new(InFlight {
            xid: request.candidate.xid.clone(),
            decision: Mutex::new(None),
            monitor: Notify::new(),
        });

        // todo: Introduce the limit of in-flight transactions.
        {
            let mut state = self.in_flight.lock().unwrap();
            state.insert(request.candidate.xid.clone(), Arc::clone(&in_flight));
        }

        let msg = CandidateMessage::new(self.config.agent_name.clone(), self.config.cohort_name.clone(), request.candidate.clone());
        let _publish_response = self.publisher.send_message(request.message_key.clone(), msg).await?;

        loop {
            if let Some(answer) = in_flight.decision.lock().unwrap().as_ref() {
                debug!("certify(): received decision for xid: {}, {:?}", request.candidate.xid, answer);
                return Ok(CertificationResponse {
                    xid: answer.xid.clone(),
                    is_accepted: answer.decision == Decision::Committed,
                });
            }
            debug!("certify(): waiting for decision on xid: {}", request.candidate.xid);
            in_flight.monitor.notified().await;

            let mut state = self.in_flight.lock().unwrap();
            state.remove(request.candidate.xid.as_str());
        }
    }
}
