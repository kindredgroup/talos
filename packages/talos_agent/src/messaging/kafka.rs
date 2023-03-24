use crate::api::{KafkaConfig, TalosType};
use crate::messaging::api::{
    CandidateMessage, ConsumerType, Decision, DecisionMessage, PublishResponse, Publisher, PublisherType, TalosMessageType, HEADER_AGENT_ID,
    HEADER_MESSAGE_TYPE,
};
use crate::messaging::errors::{MessagingError, MessagingErrorKind};
use async_trait::async_trait;
use log::{debug, error, info, warn};
use rdkafka::consumer::{Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{Header, Headers, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, ClientContext, Message, Offset, TopicPartitionList};
use std::collections::HashMap;
use std::str::Utf8Error;
use std::sync::Arc;
use std::time::Duration;
use std::{str, thread};
use time::OffsetDateTime;

/// The Kafka error into generic custom messaging error type

impl From<KafkaError> for MessagingError {
    fn from(e: KafkaError) -> Self {
        MessagingError {
            kind: MessagingErrorKind::Generic,
            reason: format!("Kafka error.\nReason: {}", e),
            cause: Some(e.to_string()),
        }
    }
}

// $coverage:ignore-start
// At this stage we cannot test external dependencies with unit tests
/// The implementation of publisher which communicates with kafka brokers.
pub struct KafkaPublisher {
    agent: String,
    config: KafkaConfig,
    producer: FutureProducer,
}

impl KafkaPublisher {
    pub fn new(agent: String, config: &KafkaConfig) -> Result<KafkaPublisher, MessagingError> {
        Ok(Self {
            agent,
            config: config.clone(),
            producer: Self::create_producer(config)?,
        })
    }

    fn create_producer(kafka: &KafkaConfig) -> Result<FutureProducer, KafkaError> {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", &kafka.brokers)
            .set("message.timeout.ms", &kafka.message_timeout_ms.to_string())
            .set("queue.buffering.max.messages", "1000000")
            .set("topic.metadata.refresh.interval.ms", "4")
            .set_log_level(kafka.log_level);

        setup_kafka_auth(&mut cfg, kafka);
        cfg.create()
    }

    pub fn make_record<'a>(agent: String, topic: &'a str, key: &'a str, message: &'a str) -> FutureRecord<'a, str, str> {
        let type_value = TalosMessageType::Candidate.to_string();
        let h_type = Header {
            key: HEADER_MESSAGE_TYPE,
            value: Some(type_value.as_str()),
        };
        let h_agent_id = Header {
            key: HEADER_AGENT_ID,
            value: Some(&agent),
        };

        FutureRecord::to(topic)
            .key(key)
            .payload(message)
            .headers(OwnedHeaders::new().insert(h_type).insert(h_agent_id))
    }
}

#[async_trait]
impl Publisher for KafkaPublisher {
    async fn send_message(&self, key: String, message: CandidateMessage) -> Result<PublishResponse, MessagingError> {
        debug!("KafkaPublisher.send_message(): async publishing message {:?} with key: {}", message, key);

        let topic = self.config.certification_topic.clone();
        let payload = serde_json::to_string(&message).unwrap();

        let data = KafkaPublisher::make_record(self.agent.clone(), &self.config.certification_topic, key.as_str(), payload.as_str());

        let timeout = Timeout::After(Duration::from_millis(self.config.enqueue_timeout_ms));
        return match self.producer.send(data, timeout).await {
            Ok((partition, offset)) => {
                debug!("KafkaPublisher.send_message(): Published into partition {}, offset: {}", partition, offset);
                Ok(PublishResponse { partition, offset })
            }
            Err((e, _)) => {
                error!("KafkaPublisher.send_message(): Error publishing xid: {}, error: {}", message.xid, e);
                Err(MessagingError::new_publishing(format!("Cannot publish into topic: {}", topic), e.to_string()))
            }
        };
    }
}

/// The implementation of consumer which receives from kafka.
pub struct KafkaConsumer {
    agent: String,
    config: KafkaConfig,
    consumer: StreamConsumer<KafkaConsumerContext>,
}

struct KafkaConsumerContext {}
impl ClientContext for KafkaConsumerContext {}
impl ConsumerContext for KafkaConsumerContext {
    #[allow(clippy::needless_lifetimes)]
    fn pre_rebalance<'a>(&self, _rebalance: &Rebalance<'a>) {
        info!("[{}] pre_rebalance()", thread::current().name().unwrap_or("-"));
    }

    #[allow(clippy::needless_lifetimes)]
    fn post_rebalance<'a>(&self, _rebalance: &Rebalance<'a>) {
        info!("[{}] post_rebalance()", thread::current().name().unwrap_or("-"));
    }
}

impl KafkaConsumer {
    pub fn new(agent: String, config: &KafkaConfig) -> Result<Self, MessagingError> {
        let consumer = Self::create_consumer(config)?;

        Ok(KafkaConsumer {
            agent,
            config: config.clone(),
            consumer,
        })
    }

    fn create_consumer(kafka: &KafkaConfig) -> Result<StreamConsumer<KafkaConsumerContext>, KafkaError> {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", &kafka.brokers)
            .set("group.id", &kafka.group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "latest")
            .set("socket.keepalive.enable", "true")
            .set("fetch.wait.max.ms", kafka.fetch_wait_max_ms.to_string())
            .set_log_level(kafka.log_level);

        setup_kafka_auth(&mut cfg, kafka);

        cfg.create_with_context(KafkaConsumerContext {})
    }

    pub fn subscribe(&self) -> Result<(), KafkaError> {
        let topic = self.config.certification_topic.as_str();
        let partition = 0_i32;

        let mut partition_list = TopicPartitionList::new();
        partition_list.add_partition(topic, partition);
        // This line is required for seek operation to be successful.
        partition_list.set_partition_offset(topic, partition, Offset::End)?;

        info!("Assigning partition list to consumer: {:?}", partition_list);
        self.consumer.assign(&partition_list)?;

        info!("Fetching offset for partition: {}", partition);
        let offset = self.get_offset(partition, Duration::from_secs(5))?;

        info!("Seeking on partition {} to offset: {:?}", partition, offset);
        self.consumer.seek(topic, partition, offset, Duration::from_secs(5))?;

        Ok(())
    }

    fn get_offset(&self, partition: i32, timeout: Duration) -> Result<Offset, KafkaError> {
        let topic = self.config.certification_topic.as_str();
        let (_low, high) = self.consumer.fetch_watermarks(topic, partition, timeout)?;

        Ok(Offset::Offset(high))
    }
}

#[async_trait]
impl crate::messaging::api::Consumer for KafkaConsumer {
    async fn receive_message(&self) -> Option<Result<DecisionMessage, MessagingError>> {
        let rslt_received = self
            .consumer
            .recv()
            .await
            .map_err(|kafka_error| MessagingError::new_consuming(kafka_error.to_string()));

        if let Err(e) = rslt_received {
            return Some(Err(e));
        }

        let received = rslt_received.unwrap();

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
        let is_id_matching = match headers.get(HEADER_AGENT_ID) {
            Some(value) => value == self.agent.as_str(),
            None => false,
        };

        if !is_id_matching {
            return None;
        }

        // Extract message type from headers
        let parsed_type = headers.get(HEADER_MESSAGE_TYPE).and_then(|raw| match TalosMessageType::try_from(raw.as_str()) {
            Ok(parsed_type) => Some(parsed_type),
            Err(parse_error) => {
                warn!(
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
                warn!("Unable to parse decisionTime from this value '{}'. Error: {:?}", raw_value, e);
                None
            }
        });

        parsed_type.and_then(|message_type| deserialize_decision(&self.config.talos_type, &message_type, &received.payload_view::<str>(), decided_at))
    }
}

/// Utilities.

fn setup_kafka_auth(client: &mut ClientConfig, config: &KafkaConfig) {
    if let Some(username) = &config.username {
        client
            .set("security.protocol", "SASL_PLAINTEXT")
            .set("sasl.mechanisms", config.sasl_mechanisms.clone().unwrap_or_else(|| "SCRAM-SHA-512".to_string()))
            .set("sasl.username", username)
            .set("sasl.password", config.password.clone().unwrap_or_default());
    }
}

fn deserialize_decision(
    talos_type: &TalosType,
    message_type: &TalosMessageType,
    payload_view: &Option<Result<&str, Utf8Error>>,
    decided_at: Option<u64>,
) -> Option<Result<DecisionMessage, MessagingError>> {
    match message_type {
        TalosMessageType::Candidate => match talos_type {
            TalosType::External => None,
            TalosType::InProcessMock => payload_view.and_then(|raw_payload| {
                Some(parse_payload_as_candidate(
                    &raw_payload,
                    Decision::Committed,
                    decided_at.or_else(|| Some(OffsetDateTime::now_utc().unix_timestamp_nanos() as u64)),
                ))
            }),
        },

        // Take only decisions...
        TalosMessageType::Decision => payload_view.and_then(|raw_payload| {
            Some(parse_payload_as_decision(&raw_payload).map(|mut decision| {
                decision.decided_at = decided_at;
                decision
            }))
        }),
    }
}

fn parse_payload_as_decision(raw_payload: &Result<&str, Utf8Error>) -> Result<DecisionMessage, MessagingError> {
    let json_as_text = raw_payload.map_err(|utf_error| MessagingError::new_corrupted_payload("Payload is not UTF8 text".to_string(), utf_error.to_string()))?;

    // convert JSON text into DecisionMessage
    serde_json::from_str::<DecisionMessage>(json_as_text)
        .map_err(|json_error| MessagingError::new_corrupted_payload("Payload is not JSON text".to_string(), json_error.to_string()))
}

fn parse_payload_as_candidate(raw_payload: &Result<&str, Utf8Error>, decision: Decision, decided_at: Option<u64>) -> Result<DecisionMessage, MessagingError> {
    let json_as_text = raw_payload.map_err(|utf_error| MessagingError::new_corrupted_payload("Payload is not UTF8 text".to_string(), utf_error.to_string()))?;

    // convert JSON text into DecisionMessage
    serde_json::from_str::<CandidateMessage>(json_as_text)
        .map_err(|json_error| MessagingError::new_corrupted_payload("Payload is not JSON text".to_string(), json_error.to_string()))
        .map(|candidate| DecisionMessage {
            xid: candidate.xid,
            agent: candidate.agent,
            cohort: candidate.cohort,
            decision,
            suffix_start: 0,
            version: 0,
            safepoint: None,
            decided_at,
        })
}

// $coverage:ignore-start
// At this stage we cannot test external dependencies with unit tests
/// Sets up connectivity to kafka broker.
pub struct KafkaInitializer {}

impl KafkaInitializer {
    /// Creates new instances of initialised and fully connected publisher and consumer
    pub async fn connect(agent: String, kafka_config: KafkaConfig) -> Result<(Arc<Box<PublisherType>>, Arc<Box<ConsumerType>>), MessagingError> {
        let kafka_publisher = KafkaPublisher::new(agent.clone(), &kafka_config)?;
        let kafka_consumer = KafkaConsumer::new(agent, &kafka_config)?;
        kafka_consumer.subscribe()?;

        let consumer: Arc<Box<ConsumerType>> = Arc::new(Box::new(kafka_consumer));
        let publisher: Arc<Box<PublisherType>> = Arc::new(Box::new(kafka_publisher));

        Ok((publisher, consumer))
    }
}
// $coverage:ignore-end

// $coverage:ignore-start
#[cfg(test)]
mod tests_error {
    use super::*;

    #[test]
    fn test_models() {
        let kafka_seek_error = KafkaError::Seek("some error".to_string());
        let error = MessagingError::from(kafka_seek_error);
        assert_eq!(error.kind, MessagingErrorKind::Generic);
        assert_eq!(error.cause, Some("Seek error: some error".to_string()));
    }
}
// $coverage:ignore-end

// $coverage:ignore-start
#[cfg(test)]
mod tests_publisher {
    use super::*;

    #[test]
    fn to_record() {
        let message = serde_json::to_string(&CandidateMessage {
            xid: "xid1".to_string(),
            agent: "agent".to_string(),
            cohort: "cohort".to_string(),
            readset: vec!["1".to_string()],
            readvers: vec![1_u64],
            snapshot: 2_u64,
            writeset: vec!["1".to_string()],
        })
        .unwrap();

        let record = KafkaPublisher::make_record("agent".to_string(), "topic", "key", message.as_str());
        assert_eq!(record.topic, "topic");
    }
}
// $coverage:ignore-end

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::config::RDKafkaLogLevel;

    #[test]
    fn test_setup_kafka_auth() {
        let mut cfg = ClientConfig::new();
        setup_kafka_auth(
            &mut cfg,
            &KafkaConfig {
                brokers: "brokers".to_string(),
                group_id: "group_id".to_string(),
                certification_topic: "certification_topic".to_string(),
                fetch_wait_max_ms: 1_u64,
                message_timeout_ms: 1_u64,
                enqueue_timeout_ms: 1_u64,
                log_level: RDKafkaLogLevel::Debug,
                talos_type: TalosType::InProcessMock,
                sasl_mechanisms: None,
                username: Some("user1".to_string()),
                password: None,
            },
        );
        assert!(check_key(&mut cfg, "security.protocol", "SASL_PLAINTEXT"));
        assert!(check_key(&mut cfg, "sasl.mechanisms", "SCRAM-SHA-512"));
        assert!(check_key(&mut cfg, "sasl.username", "user1"));
        assert!(check_key(&mut cfg, "sasl.password", ""));

        cfg.set("sasl.password", "pwd".to_string());
        assert!(check_key(&mut cfg, "sasl.password", "pwd"));

        cfg.set("sasl.mechanisms", "ANONYMOUS".to_string());
        assert!(check_key(&mut cfg, "sasl.mechanisms", "ANONYMOUS"));
    }

    fn check_key(cfg: &mut ClientConfig, key: &str, value: &str) -> bool {
        if let Some(v) = cfg.get(key) {
            v == value
        } else {
            false
        }
    }
}
// $coverage:ignore-end
