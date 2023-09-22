use std::collections::HashMap;

use async_trait::async_trait;
use log::{debug, info};
use rdkafka::{
    config::{FromClientConfig, FromClientConfigAndContext},
    producer::{BaseRecord, DefaultProducerContext, ProducerContext, ThreadedProducer},
    ClientContext, Message,
};
use talos_certifier::{
    errors::SystemServiceError,
    ports::{common::SharedPortTraits, errors::MessagePublishError, MessagePublisher},
};
use talos_certifier_adapters::kafka::utils::build_kafka_headers;
use talos_rdkafka_utils::kafka_config::KafkaConfig;
use tokio::sync::mpsc;

use crate::core::MessengerChannelFeedback;
use futures_executor::block_on;

#[derive(Debug)]
pub struct MessengerProducerDeliveryOpaque {
    pub version: u64,
    pub total_publish_count: u32,
}

pub struct MessengerKafkaProducerContext {
    pub tx_feedback_channel: mpsc::Sender<MessengerChannelFeedback>,
}

impl ClientContext for MessengerKafkaProducerContext {}
impl ProducerContext for MessengerKafkaProducerContext {
    type DeliveryOpaque = Box<MessengerProducerDeliveryOpaque>;

    fn delivery(&self, delivery_result: &rdkafka::producer::DeliveryResult<'_>, delivery_opaque: Self::DeliveryOpaque) {
        let result = delivery_result.as_ref();

        let version = delivery_opaque.version;

        match result {
            Ok(msg) => {
                info!("Message {:?} {:?}", msg.key(), msg.offset());
                // TODO: GK - what to do on error? Panic?
                let _ = block_on(self.tx_feedback_channel.send(MessengerChannelFeedback::Success(
                    version,
                    "kafka".to_string(),
                    delivery_opaque.total_publish_count,
                )));
            }
            Err(err) => {
                // TODO: GK - what to do on error? Panic?
                let _ = block_on(self.tx_feedback_channel.send(MessengerChannelFeedback::Error(version, err.0.to_string())));
            }
        }
    }
}

// Kafka Producer
// #[derive(Clone)]
pub struct KafkaProducer<C: ProducerContext + 'static = DefaultProducerContext> {
    producer: ThreadedProducer<C>,
    topic: String,
}

impl KafkaProducer {
    pub fn new(config: &KafkaConfig) -> Self {
        let client_config = config.build_producer_config();
        let producer = ThreadedProducer::from_config(&client_config).expect("Failed to create producer");
        let topic = config.topic.to_owned();

        Self { producer, topic }
    }
}
impl<C: ProducerContext + 'static> KafkaProducer<C> {
    pub fn with_context(config: &KafkaConfig, context: C) -> Self {
        let client_config = config.build_producer_config();
        let producer = ThreadedProducer::from_config_and_context(&client_config, context).expect("Failed to create producer");
        let topic = config.topic.to_owned();

        Self { producer, topic }
    }

    pub fn publish_to_topic(
        &self,
        topic: &str,
        key: &str,
        value: &str,
        headers: Option<HashMap<String, String>>,
        delivery_opaque: C::DeliveryOpaque,
    ) -> Result<(), SystemServiceError> {
        let record = BaseRecord::with_opaque_to(topic, delivery_opaque).payload(value).key(key);

        let record = match headers {
            Some(x) => record.headers(build_kafka_headers(x)),
            None => record,
        };

        Ok(self
            .producer
            .send(record)
            // .send(record, Timeout::After(Duration::from_secs(1)))
            // .await
            .map_err(|(kafka_error, record)| MessagePublishError {
                reason: kafka_error.to_string(),
                data: Some(format!(
                    "Topic={:?} partition={:?} key={:?} headers={:?} payload={:?}",
                    self.topic, record.partition, record.key, record.headers, record.payload
                )),
            })?)
    }
}

//  Message publisher traits
#[async_trait]
impl MessagePublisher for KafkaProducer {
    async fn publish_message(&self, key: &str, value: &str, headers: Option<HashMap<String, String>>) -> Result<(), SystemServiceError> {
        let record = BaseRecord::to(&self.topic).payload(value).key(key);

        let record = match headers {
            Some(x) => record.headers(build_kafka_headers(x)),
            None => record,
        };

        debug!("Preparing to send the Decision Message. ");
        let delivery_result = self
            .producer
            .send(record)
            // .send(record, Timeout::After(Duration::from_secs(1)))
            // .await
            .map_err(|(kafka_error, record)| MessagePublishError {
                reason: kafka_error.to_string(),
                data: Some(format!("{:?}", record)),
            })?;

        debug!("Sent the Decision Message successfully {:?} ", delivery_result.to_owned());
        Ok(())
    }
}

#[async_trait]
impl SharedPortTraits for KafkaProducer {
    async fn is_healthy(&self) -> bool {
        true
    }
    async fn shutdown(&self) -> bool {
        true
    }
}
