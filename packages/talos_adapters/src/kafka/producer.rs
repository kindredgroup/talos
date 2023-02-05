use std::collections::HashMap;

use async_trait::async_trait;
use log::{debug, info};
use rdkafka::producer::{BaseRecord, DefaultProducerContext, ThreadedProducer};
use talos_core::{
    errors::SystemServiceError,
    ports::{common::SharedPortTraits, errors::MessagePublishError, MessagePublisher},
};

use crate::kafka::utils::build_kafka_headers;

use super::{config::Config, utils::kafka_topic_prefixed};

// Kafka Producer
// #[derive(Clone)]
pub struct KafkaProducer {
    producer: ThreadedProducer<DefaultProducerContext>,
    topic: String,
}

impl KafkaProducer {
    pub fn new(config: &Config) -> Self {
        let producer = config.build_producer_config().create().expect("Failed to create producer");
        let topic = kafka_topic_prefixed(&config.producer_topic, &config.topic_prefix);

        Self { producer, topic }
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

        info!("Sent the Decision Message successfully {:?} ", delivery_result.to_owned());
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
