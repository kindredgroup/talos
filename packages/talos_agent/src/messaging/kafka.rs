use crate::api::TalosType;
use crate::messaging::api::{
    CandidateMessage, ConsumerType, PublishResponse, Publisher, PublisherType, TalosMessageType, HEADER_AGENT_ID, HEADER_MESSAGE_TYPE,
};
use crate::messaging::errors::{MessagingError, MessagingErrorKind};
use crate::messaging::message_parser::MessageReceiver;
use async_trait::async_trait;
use opentelemetry::metrics::Gauge;
use opentelemetry::{global, Context, KeyValue};
use rdkafka::consumer::{Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{Header, Headers, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{ClientContext, Offset, TopicPartitionList};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::{str, thread};
use talos_common_utils::otel::metric_constants::{
    METRIC_KEY_CERT_DECISION_TYPE, METRIC_KEY_CERT_MESSAGE_TYPE, METRIC_METER_NAME_COHORT_SDK, METRIC_NAME_CERTIFICATION_OFFSET,
    METRIC_VALUE_CERT_DECISION_TYPE_UNKNOWN, METRIC_VALUE_CERT_MESSAGE_TYPE_CANDIDATE, METRIC_VALUE_CERT_MESSAGE_TYPE_DECISION,
};
use talos_common_utils::otel::propagated_context::PropagatedSpanContextData;
use talos_rdkafka_utils::kafka_config::KafkaConfig;
use time::OffsetDateTime;
use tracing::{debug, error, info};

use super::api::ReceivedMessage;
use super::message_parser::MessageListener;

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
    metric_consumed_offset: Option<Gauge<u64>>,
}

impl KafkaPublisher {
    pub fn new(agent: String, config: &KafkaConfig) -> Result<KafkaPublisher, MessagingError> {
        let metric_consumed_offset = Some(
            global::meter(METRIC_METER_NAME_COHORT_SDK)
                .u64_gauge(format!("cohort_sdk_{}", METRIC_NAME_CERTIFICATION_OFFSET))
                .build(),
        );
        Ok(Self {
            agent,
            config: config.clone(),
            producer: config.build_producer_config().create()?,
            metric_consumed_offset,
        })
    }

    pub fn make_record<'a>(agent: String, topic: &'a str, key: &'a str, message: &'a str, extra_headers: Option<OwnedHeaders>) -> FutureRecord<'a, str, str> {
        let type_value = TalosMessageType::Candidate.to_string();
        let h_type = Header {
            key: HEADER_MESSAGE_TYPE,
            value: Some(type_value.as_str()),
        };
        let h_agent_id = Header {
            key: HEADER_AGENT_ID,
            value: Some(&agent),
        };

        let mut headers = OwnedHeaders::new().insert(h_type).insert(h_agent_id);
        if let Some(ehs) = extra_headers {
            for eh in ehs.iter() {
                headers = headers.insert(eh);
            }
        }

        FutureRecord::to(topic).key(key).payload(message).headers(headers)
    }
}

#[async_trait]
impl Publisher for KafkaPublisher {
    async fn send_message(
        &self,
        key: String,
        mut message: CandidateMessage,
        headers: Option<HashMap<String, String>>,
        parent_span_ctx: Option<Context>,
    ) -> Result<PublishResponse, MessagingError> {
        debug!(
            "KafkaPublisher.send_message(): async publishing message with headers {:?} and key: {}",
            headers, key
        );

        let topic = self.config.topic.clone();

        let opt_ctx_data = parent_span_ctx.map(|ctx| PropagatedSpanContextData::new_with_otel_context(&ctx).get_data());
        let mut extra_headers = OwnedHeaders::new();
        let mut have_headers = false;
        if let Some(hset) = headers {
            for (k, v) in hset.into_iter() {
                extra_headers = extra_headers.insert(Header {
                    key: k.as_str(),
                    value: Some(v.as_str()),
                });
                have_headers = true;
            }
        }

        if let Some(ctx_data) = opt_ctx_data {
            for (k, v) in ctx_data.into_iter() {
                extra_headers = extra_headers.insert(Header {
                    key: k.as_str(),
                    value: Some(v.as_str()),
                });
                have_headers = true;
            }
        };

        message.published_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let payload = serde_json::to_string(&message).unwrap();

        let data = KafkaPublisher::make_record(
            self.agent.clone(),
            &self.config.topic,
            key.as_str(),
            payload.as_str(),
            if have_headers { Some(extra_headers) } else { None },
        );

        let timeout = Timeout::After(Duration::from_millis(self.config.producer_send_timeout_ms.unwrap_or(10) as u64));

        return match self.producer.send(data, timeout).await {
            Ok((partition, offset)) => {
                debug!("KafkaPublisher.send_message(): Published into partition {}, offset: {}", partition, offset);
                if let Some(metric) = &self.metric_consumed_offset {
                    metric.record(
                        offset as u64,
                        &[
                            KeyValue::new(METRIC_KEY_CERT_MESSAGE_TYPE, METRIC_VALUE_CERT_MESSAGE_TYPE_CANDIDATE),
                            KeyValue::new(METRIC_KEY_CERT_DECISION_TYPE, METRIC_VALUE_CERT_DECISION_TYPE_UNKNOWN),
                        ],
                    );
                }
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
    talos_type: TalosType,
    metric_consumed_offset: Option<Gauge<u64>>,
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
    pub fn new(agent: String, config: &KafkaConfig, talos_type: TalosType) -> Result<Self, MessagingError> {
        let consumer = Self::create_consumer(config)?;
        let metric_consumed_offset = Some(
            global::meter(METRIC_METER_NAME_COHORT_SDK)
                .u64_gauge(format!("cohort_sdk_{}", METRIC_NAME_CERTIFICATION_OFFSET))
                .build(),
        );
        Ok(KafkaConsumer {
            agent,
            config: config.clone(),
            consumer,
            talos_type,
            metric_consumed_offset,
        })
    }

    fn create_consumer(kafka_config: &KafkaConfig) -> Result<StreamConsumer<KafkaConsumerContext>, KafkaError> {
        let cfg = kafka_config.build_consumer_config();
        cfg.create_with_context(KafkaConsumerContext {})
    }

    pub fn subscribe(&self) -> Result<(), KafkaError> {
        let topic = &self.config.topic.as_str();
        let partition = 0_i32;

        let mut partition_list = TopicPartitionList::new();
        partition_list.add_partition(topic, partition);
        // This line is required for seek operation to be successful.
        partition_list.set_partition_offset(topic, partition, Offset::End)?;

        debug!("Assigning partition list to consumer: {:?}", partition_list);
        self.consumer.assign(&partition_list)?;

        debug!("Fetching offset for partition: {}", partition);
        let offset = self.get_offset(partition, Duration::from_secs(5))?;

        debug!("Seeking on partition {} to offset: {:?}", partition, offset);
        self.consumer.seek(topic, partition, offset, Duration::from_secs(5))?;

        Ok(())
    }

    fn get_offset(&self, partition: i32, timeout: Duration) -> Result<Offset, KafkaError> {
        let topic = &self.config.topic.as_str();
        let (_low, high) = self.consumer.fetch_watermarks(topic, partition, timeout)?;

        Ok(Offset::Offset(high))
    }
}

#[async_trait]
impl crate::messaging::api::Consumer for KafkaConsumer {
    fn get_talos_type(&self) -> TalosType {
        self.talos_type.clone()
    }

    async fn receive_message(&self) -> Result<ReceivedMessage, MessagingError> {
        let received = self
            .consumer
            .recv()
            .await
            .map_err(|kafka_error| MessagingError::new_consuming(kafka_error.to_string()))?;

        let mut parser = MessageReceiver::new(self.agent.as_str(), self.get_talos_type(), &received);
        parser.add_listener(self);
        let _ = parser.read_headers()?;
        parser.read_content()
    }
}

impl MessageListener for KafkaConsumer {
    fn on_candidate(&self, message: &ReceivedMessage) {
        let offset = message.offset;
        // ok this is totally unexpected state. Did kafka topic reset? Is kafka re-delivering old message?
        let message_time_info = message.timestamp.map(|millis| {
            let message_time = OffsetDateTime::from_unix_timestamp(millis);
            if message_time.is_ok() {
                let now = OffsetDateTime::now_utc();
                let elapsed = (now.unix_timestamp_nanos() - message_time.unwrap().unix_timestamp_nanos()) as f64 / 1_000_000_f64;
                format!("(time: {}, elapsed: {}ms)", message_time.unwrap(), elapsed)
            } else {
                "no-data".to_owned()
            }
        });
        tracing::debug!("Agent is receiving candidate with offset: {} and timestamp: {:?}", offset, message_time_info);

        if let Some(m) = self.metric_consumed_offset.clone() {
            m.record(
                offset,
                &[
                    KeyValue::new(METRIC_KEY_CERT_MESSAGE_TYPE, METRIC_VALUE_CERT_MESSAGE_TYPE_CANDIDATE),
                    KeyValue::new(METRIC_KEY_CERT_DECISION_TYPE, METRIC_VALUE_CERT_DECISION_TYPE_UNKNOWN),
                ],
            )
        }
    }

    fn on_decision(&self, message: &ReceivedMessage) {
        if self.metric_consumed_offset.is_none() {
            return;
        }

        let metric = self.metric_consumed_offset.clone().unwrap();

        let decision_value = match &message.decision {
            None => METRIC_VALUE_CERT_DECISION_TYPE_UNKNOWN.to_owned(),
            Some(td) => td.decision.decision.to_string(),
        };

        metric.record(
            message.offset,
            &[
                KeyValue::new(METRIC_KEY_CERT_MESSAGE_TYPE, METRIC_VALUE_CERT_MESSAGE_TYPE_DECISION.to_string()),
                KeyValue::new(METRIC_KEY_CERT_DECISION_TYPE, decision_value),
            ],
        );
    }
}

// $coverage:ignore-start
// At this stage we cannot test external dependencies with unit tests
/// Sets up connectivity to kafka broker.
pub struct KafkaInitializer {}

impl KafkaInitializer {
    /// Creates new instances of initialised and fully connected publisher and consumer
    pub async fn connect(
        agent: String,
        kafka_config: KafkaConfig,
        talos_type: TalosType,
    ) -> Result<(Arc<Box<PublisherType>>, Arc<Box<ConsumerType>>), MessagingError> {
        let kafka_publisher = KafkaPublisher::new(agent.clone(), &kafka_config)?;
        let kafka_consumer = KafkaConsumer::new(agent, &kafka_config, talos_type)?;
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
            statemap: None,
            on_commit: None,
            certification_started_at: 0,
            request_created_at: 0,
            published_at: 0,
        })
        .unwrap();

        let record = KafkaPublisher::make_record("agent".to_string(), "topic", "key", message.as_str(), None);
        assert_eq!(record.topic, "topic");
    }
}
// $coverage:ignore-end
