use multimap::MultiMap;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use time::OffsetDateTime;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Notify;

use crate::agentv2::model::{CancelRequestChannelMessage, CertifyRequestChannelMessage};

use crate::api::{AgentConfig, CertificationResponse, KafkaConfig, TalosIntegrationType, TRACK_PUBLISH_LATENCY};
use crate::messaging::api::{CandidateMessage, ConsumerType, Decision, DecisionMessage, PublisherType};

use crate::messaging::kafka::{KafkaConsumer, KafkaPublisher};
use crate::messaging::mock::{MockConsumer, MockPublisher};

struct WaitingClient {
    received_at: u64,
    tx_sender: Sender<CertificationResponse>,
}

impl WaitingClient {
    async fn notify(&self, response: CertificationResponse, error_message: String) {
        if let Err(e) = self.tx_sender.send(response).await {
            log::error!("{}. Error: {}", error_message, e);
        };
    }
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
        publish_times: Arc<Mutex<HashMap<String, u64>>>,
    ) -> StateManager {
        StateManager {
            agent_config,
            kafka_config,
            int_type: int_type.clone(),
            publish_times,
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

        let (tx_decision, mut rx_decision) = tokio::sync::mpsc::channel::<DecisionMessage>(agent_config.buffer_size);

        let it_for_consumer = self.int_type.clone();
        let config_for_consumer = config.clone();

        let consumer_barrier = Arc::new(Notify::new());
        let consumer_barrier_arc = Arc::clone(&consumer_barrier);

        tokio::spawn(async move {
            let consumer: Box<ConsumerType> = match it_for_consumer {
                TalosIntegrationType::Kafka => {
                    let result = KafkaConsumer::new_subscribed(agent, &config_for_consumer);
                    log::info!("Created kafka consumer");
                    result
                }
                TalosIntegrationType::InMemory => Self::create_mock_consumer(&config_for_consumer),
            }
            .map_err(|e| {
                log::error!("{}", e);
                e
            })
            .unwrap();

            consumer_barrier_arc.notify_one();

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
                    log::info!("Created kafka publisher");
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
                    // end of receive certification request from agent

                    rslt_cancel_request_msg = rx_cancel.recv() => {
                        match rslt_cancel_request_msg {
                            // This is timeout
                            Some(cancel_msg) => {
                                log::warn!("The candidate '{}' is cancelled.", &cancel_msg.request.candidate.xid);
                                state.remove(&cancel_msg.request.candidate.xid)
                            },
                            None => break,
                        };
                    }
                    // end of receive request cancellation from agent

                    rslt_decision_msg = rx_decision.recv() => {
                        match rslt_decision_msg {
                            Some(decision_msg) => {
                                let xid = &decision_msg.xid;
                                Self::reply_to_agent(xid, decision_msg.decision, decision_msg.decided_at, state.get_vec(&decision_msg.xid)).await;
                                state.remove(xid);
                            }
                            None => break
                        }
                    }
                    // end of receive decision from kafka via channel
                }
            }
        });

        // wait until consumer is ready
        log::info!("Waiting until consumer is ready...");
        consumer_barrier.notified().await;
        log::info!("Consumer is ready.");
    }

    async fn reply_to_agent(xid: &str, decision: Decision, decided_at: Option<u64>, clients: Option<&Vec<WaitingClient>>) {
        match clients {
            Some(waiting_clients) => {
                let count = waiting_clients.len();
                let mut client_nr = 0;
                for waiting_client in waiting_clients {
                    client_nr += 1;
                    let response = CertificationResponse {
                        xid: xid.to_string(),
                        decision: decision.clone(),
                        send_started_at: waiting_client.received_at,
                        decided_at: decided_at.unwrap_or(0),
                        decision_buffered_at: OffsetDateTime::now_utc().unix_timestamp_nanos() as u64,
                        received_at: 0,
                    };

                    let error_message = format!(
                        "Error processing XID: {}. Unable to send response {} of {} to waiting client.",
                        xid, client_nr, count,
                    );

                    waiting_client.notify(response.clone(), error_message).await;
                }
            }

            None => {
                log::warn!("There are no waiting clients for the candidate '{}'", xid);
            }
        };
    }
}
