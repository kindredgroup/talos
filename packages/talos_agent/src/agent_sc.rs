use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

use crate::api::{AgentConfig, CertificationRequest, CertificationResponse, KafkaConfig, TalosAgent};
use crate::messaging::api::{CandidateMessage, ConsumerType, Decision, DecisionMessage, PublisherType};
use crate::messaging::kafka::KafkaConsumer;

pub struct TalosAgentSingleConsumerImpl {
    pub config: AgentConfig,
    pub kafka_config: Option<KafkaConfig>,
    pub publisher: Box<PublisherType>,
    pub in_flight: Arc<Mutex<HashMap<String, Arc<InFlight>>>>,
}

// #[derive(Debug, Clone, Eq, PartialEq)]
pub struct InFlight {
    pub xid: String,
    pub decision: Mutex<Option<Arc<DecisionMessage>>>,
    pub monitor: Notify,
}

impl TalosAgentSingleConsumerImpl {
    pub fn new(config: AgentConfig, kafka_config: Option<KafkaConfig>, publisher: Box<PublisherType>) -> Self {
        let agent = TalosAgentSingleConsumerImpl {
            config,
            kafka_config,
            publisher,
            in_flight: Arc::new(Mutex::new(HashMap::new())),
        };

        agent.start().unwrap();
        agent
    }

    fn start(&self) -> Result<(), String> {
        let config = self.kafka_config.clone().expect("Kafka configuration is required");
        let in_flight = Arc::clone(&self.in_flight);

        tokio::spawn(async move {
            let kc = KafkaConsumer::new(&config);
            let rc: Result<Box<ConsumerType>, String> = match kc.subscribe() {
                Ok(()) => Ok(Box::new(kc)),
                Err(e) => Err(e),
            };

            let consumer: Box<ConsumerType> = rc.unwrap();

            loop {
                println!("\nentering the receiver loop...");
                match consumer.receive_message().await {
                    Some(Ok(received_message)) => {
                        println!("received message {}", received_message.xid);

                        let state = in_flight.lock().unwrap();

                        // check if message pending decision by this agent
                        match state.get(received_message.xid.as_str()) {
                            None => {
                                println!("-- we received someone else decision message {}", received_message.xid);
                            }
                            Some(pending) => {
                                println!("we received decision message to candidate posted earlier {}", received_message.xid);
                                let mut decision = pending.decision.lock().unwrap();
                                *decision = Some(Arc::new(received_message));
                                pending.monitor.notify_one();
                            }
                        }
                    }
                    _ => {
                        println!("receiver loop is getting nothing");
                        continue;
                    }
                }

                println!("continuing the receiver loop...");
            }
        });

        Ok(())
    }
}

#[async_trait]
impl TalosAgent for TalosAgentSingleConsumerImpl {
    async fn certify(&self, request: CertificationRequest) -> Result<CertificationResponse, String> {
        let in_flight = Arc::new(InFlight {
            xid: request.candidate.xid.clone(),
            decision: Mutex::new(None),
            monitor: Notify::new(),
        });

        {
            let mut state = self.in_flight.lock().unwrap();
            state.insert(request.candidate.xid.clone(), Arc::clone(&in_flight));
        }

        let msg = CandidateMessage::new(self.config.agent_name.clone(), self.config.cohort_name.clone(), request.candidate.clone());
        let _publish_response = self.publisher.send_message(request.message_key.clone(), msg).await?;

        loop {
            if let Some(answer) = in_flight.decision.lock().unwrap().as_ref() {
                println!("received decision on {}, {:?}", request.candidate.xid, answer);
                return Ok(CertificationResponse {
                    xid: answer.xid.clone(),
                    is_accepted: answer.decision == Decision::Committed,
                    polled_total: 0,
                    polled_empty: 0,
                    polled_others: 0,
                });
            }
            println!("waiting for decision on {}", request.candidate.xid);
            in_flight.monitor.notified().await;
        }
    }
}
