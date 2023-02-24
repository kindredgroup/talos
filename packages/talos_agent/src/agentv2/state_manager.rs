use multimap::MultiMap;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use time::OffsetDateTime;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::agentv2::model::{CancelRequestChannelMessage, CertifyRequestChannelMessage};

use crate::api::{AgentConfig, CertificationResponse, KafkaConfig, TalosIntegrationType, TRACK_PUBLISH_LATENCY};
use crate::messaging::api::{CandidateMessage, ConsumerType, Decision, DecisionMessage, PublisherType};

use crate::messaging::kafka::{KafkaConsumer, KafkaPublisher};
use crate::messaging::mock::{MockConsumer, MockPublisher};

struct WaitingClient {
    received_at: u64,
    tx_sender: Sender<CertificationResponse>,
}

pub struct StateManager {
    agent_config: AgentConfig,
    kafka_config: Option<KafkaConfig>,
    int_type: TalosIntegrationType,
    publish_times: Arc<Mutex<HashMap<String, u64>>>,
}

impl StateManager {
    pub fn new(
        agent_config: AgentConfig,
        kafka_config: Option<KafkaConfig>,
        int_type: &TalosIntegrationType,
        publish_times: &Arc<Mutex<HashMap<String, u64>>>,
    ) -> StateManager {
        StateManager {
            agent_config,
            kafka_config,
            int_type: int_type.clone(),
            publish_times: Arc::clone(publish_times),
        }
    }

    fn create_mock_consumer(_config: &KafkaConfig) -> Result<Box<ConsumerType>, String> {
        Ok(Box::new(MockConsumer {}))
    }

    pub async fn start(&self, mut rx_certify: Receiver<CertifyRequestChannelMessage>, mut rx_cancel: Receiver<CancelRequestChannelMessage>) {
        let agent = self.agent_config.agent.clone();
        let agent_config = self.agent_config.clone();
        let config = self.kafka_config.clone().expect("Kafka configuration is required");
        let it = self.int_type.clone();

        let (tx_decision, mut rx_decision) = tokio::sync::mpsc::channel::<DecisionMessage>(10_000);

        let it_for_consumer = self.int_type.clone();
        let config_for_consumer = config.clone();
        tokio::spawn(async move {
            let consumer: Box<ConsumerType> = match it_for_consumer {
                TalosIntegrationType::Kafka => KafkaConsumer::new_subscribed(agent, &config_for_consumer),
                TalosIntegrationType::InMemory => Self::create_mock_consumer(&config_for_consumer),
            }
            .unwrap();

            loop {
                let rslt_decision_msg = consumer.receive_message().await;
                match rslt_decision_msg {
                    Some(Ok(decision_msg)) => {
                        match tx_decision.send(decision_msg).await {
                            Ok(()) => continue,
                            Err(e) => {
                                log::error!("Unable to send decision message over internal channel to main loop {:?}", e);
                            }
                        };
                    }

                    _ => continue, // just take next message
                }
            }
        });

        let publish_times = Arc::clone(&self.publish_times);
        tokio::spawn(async move {
            let mut state: MultiMap<String, WaitingClient> = MultiMap::new();

            let publisher: Arc<Box<PublisherType>> = match it {
                TalosIntegrationType::Kafka => {
                    let kafka_publisher = KafkaPublisher::new(agent_config.agent.clone(), &config);
                    Arc::new(Box::new(kafka_publisher))
                }
                TalosIntegrationType::InMemory => Arc::new(Box::new(MockPublisher {})),
            };

            loop {
                tokio::select! {

                    rslt_request_msg = rx_certify.recv() => {
                        match rslt_request_msg {
                            Some(request_msg) => {
                                state.insert(request_msg.request.candidate.xid.clone(), WaitingClient {
                                    received_at: OffsetDateTime::now_utc().unix_timestamp_nanos() as u64,
                                    tx_sender: request_msg.tx_answer,
                                });

                                let msg = CandidateMessage::new(
                                    agent_config.agent.clone(),
                                    agent_config.cohort.clone(),
                                    request_msg.request.candidate.clone()
                                );

                                let publisher_ref = Arc::clone(&publisher);
                                let key = request_msg.request.message_key.clone();
                                let xid = msg.xid.clone();
                                let times = Arc::clone(&publish_times);
                                tokio::spawn(async move {
                                    publisher_ref.send_message(key, msg).await.unwrap();
                                    if TRACK_PUBLISH_LATENCY {
                                        let published_at = OffsetDateTime::now_utc().unix_timestamp_nanos() as u64;
                                        match times.lock() {
                                            Ok(mut map) => {
                                                map.insert(xid, published_at);
                                            },
                                            Err(e) => {
                                                log::error!("Unable to insert publish time for xid: {}. {:?}", xid, e.to_string());
                                            }
                                        }
                                    }

                                });
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

                    rslt_decision_msg = rx_decision.recv() => {
                        match rslt_decision_msg {
                            Some(decision_msg) => {
                                let xid = &decision_msg.xid;
                                match Self::reply_to_agent(xid, decision_msg.decision, decision_msg.decided_at, state.get_vec(&decision_msg.xid)).await {
                                    Some(()) => {
                                        state.remove(xid);
                                    }
                                    None => {
                                        log::debug!("Skipping not in-flight transaction: {}", xid);
                                    }
                                }
                            }
                            None => break
                        }
                    }
                    // end of receive decision from kafka via channel
                }
            }
        });
    }

    async fn reply_to_agent(xid: &str, decision: Decision, decided_at: Option<u64>, tx: Option<&Vec<WaitingClient>>) -> Option<()> {
        match tx {
            Some(waiting_clients) => {
                let count = waiting_clients.len();
                let mut c = 0;
                for waiting_client in waiting_clients {
                    c += 1;
                    let response = CertificationResponse {
                        xid: xid.to_string(),
                        decision: decision.clone(),
                        send_started_at: waiting_client.received_at,
                        decided_at: decided_at.unwrap_or(0),
                        decision_buffered_at: OffsetDateTime::now_utc().unix_timestamp_nanos() as u64,
                        received_at: 0,
                    };
                    match waiting_client.tx_sender.send(response.clone()).await {
                        Ok(()) => {}
                        Err(e) => {
                            log::error!(
                                "Error processing XID: {}. Unable to send response {} of {} to the client. Error: {}",
                                xid,
                                c,
                                count,
                                e,
                            );
                        }
                    };
                }

                Some(())
            }

            None => None,
        }
    }
}
