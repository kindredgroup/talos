use crate::api::KafkaConfig;
use crate::messaging::api::{
    CandidateMessage, ConsumerType, DecisionMessage, PublishResponse, Publisher, TalosMessageType, HEADER_AGENT_ID, HEADER_MESSAGE_TYPE,
};
use async_trait::async_trait;
use log::debug;
use log::error;
use rdkafka::consumer::{Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::message::{Header, Headers, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, ClientContext, Message};
use std::collections::HashMap;
use std::str::Utf8Error;
use std::time::Duration;
use std::{str, thread};

/// The implementation of publisher which communicates with kafka brokers.

pub struct KafkaPublisher {
    agent_id: String,
    config: KafkaConfig,
    producer: FutureProducer,
}

impl KafkaPublisher {
    pub fn new(agent_id: String, config: &KafkaConfig) -> KafkaPublisher {
        Self {
            agent_id,
            config: config.clone(),
            producer: Self::create_producer(config),
        }
    }

    fn create_producer(kafka: &KafkaConfig) -> FutureProducer {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", &kafka.brokers)
            .set("message.timeout.ms", &kafka.message_timeout_ms.to_string())
            .set("queue.buffering.max.messages", "1000000")
            .set_log_level(kafka.log_level);

        cfg.create().expect("Unable to create kafka producer")
    }
}

#[async_trait]
impl Publisher for KafkaPublisher {
    async fn send_message(&self, key: String, message: CandidateMessage) -> Result<PublishResponse, String> {
        debug!("KafkaPublisher.send_message(): async publishing message {:?} with key: {}", message, key);

        let type_value = TalosMessageType::Candidate.to_string();
        let h_type = Header {
            key: HEADER_MESSAGE_TYPE,
            value: Some(type_value.as_str()),
        };
        let h_agent_id = Header {
            key: HEADER_AGENT_ID,
            value: Some(self.agent_id.as_str()),
        };
        let payload = serde_json::to_string(&message).unwrap();

        let data = FutureRecord::to(self.config.certification_topic.as_str())
            .key(&key)
            .payload(&payload)
            .headers(OwnedHeaders::new().insert(h_type).insert(h_agent_id));

        let timeout = Timeout::After(Duration::from_millis(self.config.enqueue_timeout_ms));
        return match self.producer.send(data, timeout).await {
            Ok((partition, offset)) => {
                debug!("KafkaPublisher.send_message(): Published into partition {}, offset: {}", partition, offset);
                Ok(PublishResponse { partition, offset })
            }
            Err((e, _)) => {
                error!("KafkaPublisher.send_message(): Error publishing xid: {}, error: {}", message.xid, e);
                Err(e.to_string())
            }
        };
    }
}

/// The implementation of consumer which receives from kafka.
pub struct KafkaConsumer {
    agent_id: String,
    config: KafkaConfig,
    consumer: StreamConsumer<KafkaConsumerContext>,
}

struct KafkaConsumerContext {}
impl ClientContext for KafkaConsumerContext {}
impl ConsumerContext for KafkaConsumerContext {
    fn pre_rebalance<'a>(&self, _rebalance: &Rebalance<'a>) {
        log::info!("[{}] pre_rebalance()", thread::current().name().unwrap_or("-"));
    }

    fn post_rebalance<'a>(&self, _rebalance: &Rebalance<'a>) {
        // thread::sleep(Duration::from_secs(5));
        log::info!("[{}] post_rebalance()", thread::current().name().unwrap_or("-"));
    }
}

impl KafkaConsumer {
    pub fn new(agent_id: String, config: &KafkaConfig) -> Self {
        KafkaConsumer {
            agent_id,
            config: config.clone(),
            consumer: Self::create_consumer(config),
        }
    }

    pub fn new_subscribed(agent_id: String, config: &KafkaConfig) -> Result<Box<ConsumerType>, String> {
        let kc = KafkaConsumer::new(agent_id, config);
        match kc.subscribe() {
            Ok(()) => Ok(Box::new(kc)),
            Err(e) => Err(e),
        }
    }

    fn create_consumer(kafka: &KafkaConfig) -> StreamConsumer<KafkaConsumerContext> {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", &kafka.brokers)
            .set("group.id", &kafka.group_id)
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "latest")
            .set("socket.keepalive.enable", "true")
            .set("auto.commit.interval.ms", "500")
            .set("fetch.wait.max.ms", kafka.fetch_wait_max_ms.to_string())
            // .set("heartbeat.interval.ms", &kafka.heartbeat_interval_ms)
            .set_log_level(kafka.log_level);

        match cfg.create_with_context(KafkaConsumerContext {}) {
            Ok(v) => v,
            Err(e) => {
                panic!("Cannot create consumer instance. {}", e)
            }
        }
    }

    pub fn subscribe(&self) -> Result<(), String> {
        match self.consumer.subscribe(&[self.config.certification_topic.as_str()]) {
            Ok(_) => {
                loop {
                    match self.consumer.fetch_group_list(None, Duration::from_secs(1)) {
                        Ok(group_list) => {
                            let list = group_list.groups();
                            let mut found = false;
                            for group_info in list {
                                if group_info.name() == self.config.group_id {
                                    log::info!("Detected consumer group: name: {}, state: {}", group_info.name(), group_info.state());

                                    if group_info.state() != "Stable" {
                                        continue;
                                    }

                                    found = true;
                                    break;
                                }
                            }

                            if found {
                                break;
                            }
                        }
                        Err(_) => {
                            log::error!("Cannot fetch group info for group id '{}'", self.config.group_id);
                            break;
                        }
                    }

                    log::info!("Consumer group is not ready yet");
                    thread::sleep(Duration::from_secs(1));
                }

                Ok(())
            }
            Err(kafka_error) => {
                error!("Error when subscribing to topics. {:?}", kafka_error);
                Err(kafka_error.to_string())
            }
        }
    }

    fn deserialize_decision(
        message_type: &TalosMessageType,
        payload_view: &Option<Result<&str, Utf8Error>>,
        decided_at: Option<u64>,
    ) -> Option<Result<DecisionMessage, String>> {
        match message_type {
            TalosMessageType::Candidate => None,

            // Take only decisions...
            TalosMessageType::Decision => payload_view.and_then(|raw_payload| {
                Some(KafkaConsumer::parse_payload_as_decision(&raw_payload).map(|mut decision| {
                    decision.decided_at = decided_at;
                    decision
                }))
            }),
        }
    }

    fn parse_payload_as_decision(raw_payload: &Result<&str, Utf8Error>) -> Result<DecisionMessage, String> {
        return match raw_payload {
            Err(payload_read_error) => {
                error!("Unable to read kafka message payload: {}", payload_read_error);
                Err(payload_read_error.to_string())
            }

            Ok(json) => {
                // convert JSON text into DecisionMessage
                serde_json::from_str::<DecisionMessage>(json).map_err(|json_error| {
                    error!("Unable to parse JSON into DecisionMessage: {}", json_error);
                    json_error.to_string()
                })
            }
        };
    }
}

#[async_trait]
impl crate::messaging::api::Consumer for KafkaConsumer {
    async fn receive_message(&self) -> Option<Result<DecisionMessage, String>> {
        let received = match self.consumer.recv().await {
            Err(kafka_error) => {
                error!("KafkaConsumer.receive_message(): error: {:?}", kafka_error);
                Some(Err(kafka_error.to_string()))
            }

            Ok(received) => {
                // Extract headers
                let headers = match received.headers() {
                    Some(bh) => {
                        let mut headers = HashMap::<String, String>::new();
                        for header in bh.iter() {
                            if let Some(v) = header.value {
                                headers.insert(header.key.to_string(), String::from_utf8_lossy(v).to_string());
                            }
                        }

                        headers
                    }
                    _ => HashMap::<String, String>::new(),
                };

                // Extract agent id from headers
                // todo: See KDT-26
                let is_id_matching = !self.agent_id.as_str().is_empty();
                // let is_id_matching = match headers.get(HEADER_AGENT_ID).and_then(|received_agent_id| {
                //    Some(received_agent_id) => received_agent_id == self.agent_id.as_str()
                //    None => false
                // });

                if !is_id_matching {
                    return None;
                }

                // Extract message type from headers
                let parsed_type = headers.get(HEADER_MESSAGE_TYPE).and_then(|raw| match TalosMessageType::try_from(raw.as_str()) {
                    Ok(parsed_type) => Some(parsed_type),
                    Err(parse_error) => {
                        error!(
                            "KafkaConsumer.receive_message(): Unknown header value messageType='{}', skipping this message: {}. Error: {}",
                            raw,
                            received.offset(),
                            parse_error
                        );
                        None
                    }
                });

                let decided_at = headers.get("decisionTime").and_then(|raw_value| match raw_value.as_str().parse::<u64>() {
                    Ok(parsed) => Some(parsed),
                    Err(e) => {
                        log::warn!("Unable to parse decisionTime from this value '{}'. Error: {:?}", raw_value, e);
                        None
                    }
                });

                parsed_type.and_then(|message_type| KafkaConsumer::deserialize_decision(&message_type, &received.payload_view::<str>(), decided_at))
            }
        };

        received
    }
}
