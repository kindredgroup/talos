use talos_certifier::ports::MessageReciever;
use talos_certifier_adapters::{kafka, KafkaConsumer};
use talos_common_utils::env_var;
use talos_messenger::messenger;
use talos_rdkafka_utils::kafka_config::KafkaConfig;

#[tokio::main]
async fn main() {
    env_logger::builder().format_timestamp_millis().init();

    // 0. Create required items.
    //  a. Create Kafka consumer
    let mut kafka_config = KafkaConfig::from_env(None);
    kafka_config.group_id = env_var!("TALOS_MESSENGER_KAFKA_GROUP_ID");
    kafka_config.extend(None, None);
    let kafka_consumer = KafkaConsumer::new(&kafka_config);

    // b. Subscribe to topic.
    kafka_consumer.subscribe().await.unwrap();
    messenger::talos_messenger(kafka_consumer).await;
}
