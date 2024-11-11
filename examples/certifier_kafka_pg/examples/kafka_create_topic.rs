use std::collections::HashMap;

use talos_certifier_adapters::kafka::kafka_deploy::{create_topic, CreateTopicConfigs, KafkaDeployError, KafkaDeployStatus};
use talos_common_utils::env_var_with_defaults;
use talos_rdkafka_utils::kafka_config::KafkaConfig;

#[tokio::main]
async fn main() -> Result<(), KafkaDeployError> {
    println!("Creating kafka topic...");

    let kafka_config = KafkaConfig::from_env(None);

    let replication_factor = env_var_with_defaults!("KAFKA_CREATE_TOPIC_REPLICATION_COUNT", Option::<i32>, 3);
    let num_partitions = env_var_with_defaults!("KAFKA_CREATE_TOPIC_PARTITIONS", Option::<i32>, 1);

    // eg: KAFKA_CREATE_TOPIC_CONFIGS="retention.ms=3600000,"
    let config_option = env_var_with_defaults!("KAFKA_CREATE_TOPIC_CONFIGS", Option::<String>);

    let mut config: HashMap<String, String> = HashMap::new();

    if let Some(config_string) = config_option {
        config_string.trim().split(',').for_each(|c| {
            if let Some((k, v)) = c.trim().split_once('=') {
                config.insert(k.to_owned(), v.to_owned());
            }
        });
    };

    let topic_config = CreateTopicConfigs {
        topic: kafka_config.topic.clone(),
        config,
        replication_factor,
        num_partitions,
    };

    let status = create_topic(&kafka_config, topic_config).await?;

    match status {
        KafkaDeployStatus::TopicExists => {
            println!("Topic already exists...!!!")
        }
        KafkaDeployStatus::TopicCreated => {
            println!("Topic created successfully !!!")
        }
    }

    Ok(())
}
