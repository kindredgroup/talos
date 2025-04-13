use ahash::HashMap;
use async_trait::async_trait;

use log::warn;
use rdkafka::{
    config::FromClientConfig,
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    Message,
};
use talos_certifier::ports::{common::SharedPortTraits, errors::MessagePublishError};
use talos_certifier_adapters::kafka::utils::build_kafka_headers;
use talos_rdkafka_utils::kafka_config::KafkaConfig;

// Kafka Producer
pub struct KafkaProducer {
    producer: FutureProducer,
}

impl KafkaProducer {
    pub fn new(config: &KafkaConfig) -> Self {
        let client_config = config.build_producer_config();
        let producer = FutureProducer::from_config(&client_config).expect("Failed to create producer");

        Self { producer }
    }

    pub async fn publish_to_topic(
        &self,
        topic: &str,
        partition: Option<i32>,
        key: Option<&str>,
        value: &str,
        headers: HashMap<String, String>,
    ) -> Result<(), MessagePublishError> {
        let new_record = FutureRecord::to(topic).payload(value);

        // Add partition if applicable
        let new_record = if let Some(part) = partition { new_record.partition(part) } else { new_record };

        // Add key if applicable
        let new_record = if let Some(key_str) = key { new_record.key(key_str) } else { new_record };

        // Add headers if applicable
        let new_record = new_record.headers(build_kafka_headers(headers.clone()));

        // Timeout::Never is used so that we don't lose the message because of queue full.
        // TODO: GK - send timeout to be configurable.
        let _ = self.producer.send(new_record, Timeout::Never).await.map_err(|(kafka_error, error_record)| {
            warn!(
                "Failed to publish message for topic = {:?} | partition = {:?} | key = {:?}. Error {kafka_error:?}",
                error_record.topic(),
                error_record.partition(),
                error_record.key(),
            );
            MessagePublishError {
                reason: kafka_error.to_string(),
                data: Some(format!(
                    "Topic={:?} partition={:?} key={:?} ",
                    error_record.topic(),
                    error_record.partition(),
                    error_record.key()
                )),
            }
        })?;

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
