use std::collections::HashMap;

use async_trait::async_trait;
use log::debug;
use rdkafka::{
    config::{FromClientConfig, FromClientConfigAndContext},
    producer::{BaseRecord, DefaultProducerContext, ProducerContext, ThreadedProducer},
};
use talos_certifier::{
    errors::SystemServiceError,
    ports::{common::SharedPortTraits, errors::MessagePublishError, MessagePublisher},
};
use talos_certifier_adapters::kafka::utils::build_kafka_headers;
use talos_rdkafka_utils::kafka_config::KafkaConfig;

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
        partition: Option<i32>,
        key: Option<&str>,
        value: &str,
        headers: Option<HashMap<String, String>>,
        delivery_opaque: C::DeliveryOpaque,
    ) -> Result<(), MessagePublishError> {
        let record = BaseRecord::with_opaque_to(topic, delivery_opaque).payload(value);

        // Add partition if applicable
        let record = if let Some(part) = partition { record.partition(part) } else { record };

        // Add key if applicable
        let record = if let Some(key_str) = key { record.key(key_str) } else { record };

        // Add headers if applicable
        let record = match headers {
            Some(x) => record.headers(build_kafka_headers(x)),
            None => record,
        };

        self.producer.send(record).map_err(|(kafka_error, record)| MessagePublishError {
            reason: kafka_error.to_string(),
            data: Some(format!(
                "Topic={:?} partition={:?} key={:?} headers={:?} payload={:?}",
                self.topic, record.partition, record.key, record.headers, record.payload
            )),
        })
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

        debug!("Preparing to publish the message. ");
        let delivery_result = self
            .producer
            .send(record)
            // .send(record, Timeout::After(Duration::from_secs(1)))
            // .await
            .map_err(|(kafka_error, record)| MessagePublishError {
                reason: kafka_error.to_string(),
                data: Some(format!("{:?}", record)),
            })?;

        debug!("Published the message successfully {:?} ", delivery_result.to_owned());
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
