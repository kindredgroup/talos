use std::{collections::HashMap, time::Duration};

use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
    types::RDKafkaErrorCode,
};

use talos_rdkafka_utils::kafka_config::KafkaConfig;
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

#[derive(Debug, Clone)]
pub struct CreateTopicConfigs<'a> {
    /// topic to create.
    pub topic: String,
    /// Topic specific configs.
    ///
    /// see: https://docs.confluent.io/platform/current/installation/configuration/topic-configs.html
    pub config: HashMap<&'a str, &'a str>,
    /// Replication count for partitions in topic. Defaults to 3.
    pub replication_factor: Option<i32>,
    /// Number of paritions for the topic. Defaults to 1.
    pub num_partitions: Option<i32>,
}

const DEFAULT_REPLICATION_FACTOR: i32 = 3;
const DEFAULT_NUM_PARTITIONS: i32 = 3;

pub async fn create_topic(kafka_config: &KafkaConfig, topic_configs: CreateTopicConfigs<'_>) -> Result<KafkaDeployStatus, KafkaDeployError> {
    println!("kafka brokers = {:?} and username = {}", kafka_config.brokers, kafka_config.username);
    println!("topic configs received from env = {topic_configs:#?}");
    let consumer: StreamConsumer = kafka_config.build_consumer_config().create()?;

    let timeout = Duration::from_secs(5);
    let metadata = consumer
        .fetch_metadata(Some(&topic_configs.topic), timeout)
        .expect("Fetching topic metadata failed");

    if !metadata.topics().is_empty() && !metadata.topics()[0].partitions().is_empty() {
        Ok(KafkaDeployStatus::TopicExists)
    } else {
        println!("Topic does not exist, creating...");

        let config: Vec<(&str, &str)> = topic_configs.config.into_iter().collect();

        let topic = NewTopic {
            name: &topic_configs.topic,
            num_partitions: topic_configs.num_partitions.unwrap_or(DEFAULT_NUM_PARTITIONS),
            replication: TopicReplication::Fixed(topic_configs.replication_factor.unwrap_or(DEFAULT_REPLICATION_FACTOR)),
            config,
        };

        let opts = AdminOptions::new().operation_timeout(Some(timeout));

        let admin: AdminClient<DefaultClientContext> = kafka_config.build_consumer_config().create()?;

        let results = admin.create_topics(&[topic], &opts).await?;
        if let Err((_, RDKafkaErrorCode::TopicAlreadyExists)) = results[0] {
            return Ok(KafkaDeployStatus::TopicExists);
        }

        results[0].as_ref().map_err(|e| KafkaDeployError::TopicCreation(topic_configs.topic, e.1))?;

        Ok(KafkaDeployStatus::TopicCreated)
    }
}
