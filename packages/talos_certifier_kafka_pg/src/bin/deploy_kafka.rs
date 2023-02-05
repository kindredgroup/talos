use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, StreamConsumer};
use std::time::Duration;
use talos_certifier::config::Config;
use talos_certifier_adapters::KakfaConfig as KafkaConfig;

#[tokio::main]
async fn main() {
    println!("deploying kafka...");

    let config = Config::from_env();
    let config_clone = config.clone();

    println!("config received from env... {:#?}", config);

    let kafka_config = KafkaConfig {
        brokers: config.kafka_brokers,
        topic_prefix: config.kafka_topic_prefix.clone(),
        consumer_topic: config.kafka_topic.clone(),
        producer_topic: config.kafka_topic,
        client_id: config.kafka_client_id,
        group_id: config.kafka_group_id,
        username: config.kafka_username,
        password: config.kafka_password,
    }; //  config.to_kafka_client_config();
    let consumer: StreamConsumer = kafka_config.build_consumer_config().create().expect("Consumer creation failed");

    let kafka_certification_topic = format!("{}ksp.certification", config.kafka_topic_prefix);
    let timeout = Duration::from_secs(1);
    let metadata = consumer
        .fetch_metadata(Some(&kafka_certification_topic), timeout)
        .expect("Fetching topic metadata failed");

    if !metadata.topics().is_empty() && !metadata.topics()[0].partitions().is_empty() {
        println!("Topic exists, stopping...");
    } else {
        println!("Topic does not exist, creating...");
        let topic = NewTopic {
            name: &kafka_certification_topic,
            num_partitions: 1,
            replication: TopicReplication::Fixed(1),
            config: vec![],
        };
        let opts = AdminOptions::new().operation_timeout(Some(timeout));

        let admin: AdminClient<DefaultClientContext> = KafkaConfig {
            brokers: config_clone.kafka_brokers,
            topic_prefix: config_clone.kafka_topic_prefix,
            consumer_topic: config_clone.kafka_topic.clone(),
            producer_topic: config_clone.kafka_topic,
            client_id: config_clone.kafka_client_id,
            group_id: config_clone.kafka_group_id,
            username: config_clone.kafka_username,
            password: config_clone.kafka_password,
        }
        .build_consumer_config()
        .create()
        .expect("Admin client creation failed");

        match admin.create_topics(&[topic], &opts).await {
            Err(error) => println!("Topic creation failed, {:?}", error),
            Ok(results) => {
                let result = &results[0];

                match result {
                    Err((_, error_code)) => println!("Topic creation failed, {:?}", error_code),
                    Ok(_) => println!("Topic created successfully"),
                }
            }
        }
    }
}
