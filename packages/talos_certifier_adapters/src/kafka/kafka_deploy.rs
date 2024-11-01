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
pub struct CreateTopicConfigs {
    /// topic to create.
    pub topic: String,
    /// Topic specific configs.
    ///
    /// see: https://docs.confluent.io/platform/current/installation/configuration/topic-configs.html
    pub config: HashMap<String, String>,
    /// Replication count for partitions in topic. Defaults to 3.
    pub replication_count: Option<i32>,
    /// Number of paritions for the topic. Defaults to 1.
    pub num_partitions: Option<i32>,
}

pub async fn create_topic(kafka_config: &KafkaConfig, topic_configs: CreateTopicConfigs) -> Result<KafkaDeployStatus, KafkaDeployError> {
    println!("kafka configs received from env... {kafka_config:#?}");
    let consumer: StreamConsumer = kafka_config.build_consumer_config().create()?;

    let timeout = Duration::from_secs(5);
    let metadata = consumer
        .fetch_metadata(Some(&topic_configs.topic), timeout)
        .expect("Fetching topic metadata failed");

    if !metadata.topics().is_empty() && !metadata.topics()[0].partitions().is_empty() {
        Ok(KafkaDeployStatus::TopicExists)
    } else {
        println!("Topic does not exist, creating...");

        let mut config: Vec<(&str, &str)> = topic_configs.config.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        config.push(("message.timestamp.type", "LogAppendTime"));

        let topic = NewTopic {
            name: &topic_configs.topic,
            num_partitions: topic_configs.num_partitions.unwrap_or(1),
            replication: TopicReplication::Fixed(topic_configs.replication_count.unwrap_or(3)),
            config,
        };

        let opts = AdminOptions::new().operation_timeout(Some(timeout));

        let admin: AdminClient<DefaultClientContext> = kafka_config.build_consumer_config().create()?;

        let results = admin.create_topics(&[topic], &opts).await?;

        results[0].as_ref().map_err(|e| KafkaDeployError::TopicCreation(topic_configs.topic, e.1))?;

        Ok(KafkaDeployStatus::TopicCreated)
    }
}
