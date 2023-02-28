use std::time::Duration;

use crate::KafkaConfig;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
    types::RDKafkaErrorCode,
};
use thiserror::Error as ThisError;

pub enum KafkaDeployStatus {
    TopicExists,
    TopicCreated,
}

#[derive(Debug, Clone, ThisError)]
pub enum KafkaDeployError {
    #[error("Failed to create topic {0} with kafka error code={1}")]
    TopicCreation(String, RDKafkaErrorCode),
    #[error(transparent)]
    KafkaError(#[from] KafkaError),
}

pub async fn create_topic() -> Result<KafkaDeployStatus, KafkaDeployError> {
    let kafka_config = KafkaConfig::from_env();
    println!("kafka configs received from env... {kafka_config:#?}");
    let consumer: StreamConsumer = kafka_config.build_consumer_config().create()?;

    let kafka_certification_topic = kafka_config.topic.to_string();
    let timeout = Duration::from_secs(1);
    let metadata = consumer
        .fetch_metadata(Some(&kafka_certification_topic), timeout)
        .expect("Fetching topic metadata failed");

    if !metadata.topics().is_empty() && !metadata.topics()[0].partitions().is_empty() {
        Ok(KafkaDeployStatus::TopicExists)
    } else {
        println!("Topic does not exist, creating...");
        let topic = NewTopic {
            name: &kafka_certification_topic,
            num_partitions: 1,
            replication: TopicReplication::Fixed(1),
            config: vec![],
        };

        let opts = AdminOptions::new().operation_timeout(Some(timeout));

        let admin: AdminClient<DefaultClientContext> = kafka_config.build_consumer_config().create()?;

        let results = admin.create_topics(&[topic], &opts).await?;

        results[0]
            .as_ref()
            .map_err(|e| KafkaDeployError::TopicCreation(kafka_certification_topic, e.1))?;

        Ok(KafkaDeployStatus::TopicCreated)
    }
}
