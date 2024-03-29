use ahash::HashMap;
use async_trait::async_trait;
use log::debug;
use rdkafka::producer::{BaseRecord, DefaultProducerContext, ThreadedProducer};
use talos_certifier::{
    errors::SystemServiceError,
    ports::{common::SharedPortTraits, errors::MessagePublishError, MessagePublisher},
};
use talos_rdkafka_utils::kafka_config::KafkaConfig;

use crate::kafka::utils::build_kafka_headers;

// Kafka Producer
// #[derive(Clone)]
pub struct KafkaProducer {
    producer: ThreadedProducer<DefaultProducerContext>,
    topic: String,
}

impl KafkaProducer {
    pub fn new(config: &KafkaConfig) -> Self {
        let producer: ThreadedProducer<DefaultProducerContext> = config.build_producer_config().create().expect("Failed to create producer");
        let topic = config.topic.to_owned();

        Self { producer, topic }
    }
}

//  Message publisher traits
#[async_trait]
impl MessagePublisher for KafkaProducer {
    async fn publish_message(&self, key: &str, value: &str, headers: HashMap<String, String>) -> Result<(), SystemServiceError> {
        let record = BaseRecord::to(&self.topic).payload(value).key(key);

        let record = record.headers(build_kafka_headers(headers));

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
