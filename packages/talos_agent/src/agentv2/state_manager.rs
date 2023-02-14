use std::collections::HashMap;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::agentv2::model::{CancelRequestChannelMessage, CertifyRequestChannelMessage};

use crate::api::{AgentConfig, CertificationResponse, KafkaConfig, TalosIntegrationType};
use crate::messaging::api::{CandidateMessage, ConsumerType, Decision, PublisherType};

use crate::messaging::kafka::{KafkaConsumer, KafkaPublisher};
use crate::messaging::mock::{MockConsumer, MockPublisher};

pub struct StateManager {
    agent_config: AgentConfig,
    kafka_config: Option<KafkaConfig>,
    int_type: TalosIntegrationType,
}

impl StateManager {
    pub fn new(agent_config: AgentConfig, kafka_config: Option<KafkaConfig>, int_type: &TalosIntegrationType) -> Result<StateManager, String> {
        let state_manager = StateManager {
            agent_config,
            kafka_config,
            int_type: int_type.clone(),
        };

        Ok(state_manager)
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

    pub async fn start(&self, mut rx_certify: Receiver<CertifyRequestChannelMessage>, mut rx_cancel: Receiver<CancelRequestChannelMessage>) {
        let agent_id = self.agent_config.agent_id.clone();
        let agent_config = self.agent_config.clone();
        let config = self.kafka_config.clone().expect("Kafka configuration is required");
        let it = self.int_type.clone();

        tokio::spawn(async move {
            let mut state: HashMap<String, Sender<CertificationResponse>> = HashMap::new();

            let consumer: Box<ConsumerType> = match it {
                TalosIntegrationType::Kafka => Self::create_kafka_consumer(agent_id, &config),
                TalosIntegrationType::InMemory => Self::create_mock_consumer(&config),
            }
            .unwrap();

            let publisher: Box<PublisherType> = match it {
                TalosIntegrationType::Kafka => {
                    let kafka_publisher = KafkaPublisher::new(agent_config.agent_id.clone(), &config);
                    Box::new(kafka_publisher)
                }
                TalosIntegrationType::InMemory => Box::new(MockPublisher {}),
            };

            loop {
                tokio::select! {

                    rslt_request_msg = rx_certify.recv() => {
                        match rslt_request_msg {
                            Some(request_msg) => {

                                // todo:
                                //      handle case when something is already enqueued on this XID
                                state.insert(
                                    request_msg.request.candidate.xid.clone(),
                                    request_msg.tx_answer
                                );

                                let msg = CandidateMessage::new(
                                    agent_config.agent_name.clone(),
                                    agent_config.cohort_name.clone(),
                                    request_msg.request.candidate.clone()
                                );

                                publisher.send_message(request_msg.request.message_key.clone(), msg).await.unwrap();

                            },
                            None => break,
                        }
                    }
                    // end of receive request cancellation from agent

                    rslt_cancel_request_msg = rx_cancel.recv() => {
                        match rslt_cancel_request_msg {
                            // This is timeout
                            Some(cancel_msg) => state.remove(&cancel_msg.request.candidate.xid),
                            None => break,
                        };
                    }
                    // end of receive certification request from agent

                    rslt_decision_msg = consumer.receive_message() => {

                        match rslt_decision_msg {
                            Some(Ok(decision_msg)) => {
                                let xid = &decision_msg.xid;
                                match Self::reply_to_agent(xid, decision_msg.decision, state.get(&decision_msg.xid)).await {
                                    Some(()) => {
                                        state.remove(xid);
                                    }
                                    None => {
                                        log::debug!("Skipping not in-flight transaction: {}", xid);
                                    }
                                }
                            },

                            _ => continue // just take next message
                        }
                    }
                    // end of receive decision from kafka
                }
            }
        });
    }

    async fn reply_to_agent(xid: &str, decision: Decision, tx: Option<&Sender<CertificationResponse>>) -> Option<()> {
        match tx {
            Some(sender) => {
                let response = CertificationResponse {
                    xid: xid.to_string(),
                    is_accepted: decision == Decision::Committed,
                };

                sender.send(response).await.unwrap();
                Some(())
            }

            None => None,
        }
    }
}