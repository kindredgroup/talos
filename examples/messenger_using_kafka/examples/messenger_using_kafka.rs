use talos_certifier::ports::MessageReciever;
use talos_certifier_adapters::{KafkaConsumer, KafkaProducer};
use talos_common_utils::env_var;
use talos_messenger::{
    services::{MessengerInboundService, PublishActionService},
    suffix::MessengerCandidate,
    talos_messenger_service::TalosMessengerService,
};
use talos_rdkafka_utils::kafka_config::KafkaConfig;
use talos_suffix::{core::SuffixConfig, Suffix};
use tokio::sync::mpsc;

use messenger_using_kafka::kafka_producer::MessengerKafkaPublisher;

#[tokio::main]
async fn main() {
    env_logger::builder().format_timestamp_millis().init();

    // 0. Create required items.
    //  a. Create Kafka consumer
    let mut kafka_config = KafkaConfig::from_env(None);
    kafka_config.group_id = env_var!("TALOS_MESSENGER_KAFKA_GROUP_ID");
    kafka_config.extend(
        None,
        Some(
            [
                ("enable.auto.commit".to_string(), "false".to_string()),
                ("auto.offset.reset".to_string(), "earliest".to_string()),
                // ("fetch.wait.max.ms".to_string(), "600".to_string()),
                // ("socket.keepalive.enable".to_string(), "true".to_string()),
                // ("acks".to_string(), "0".to_string()),
            ]
            .into(),
        ),
    );
    let kafka_consumer = KafkaConsumer::new(&kafka_config);

    // b. Subscribe to topic.
    kafka_consumer.subscribe().await.unwrap();

    let (tx_feedback_channel, rx_feedback_channel) = mpsc::channel(10_000);
    let (tx_actions_channel, rx_actions_channel) = mpsc::channel(10_000);

    let suffix_config = SuffixConfig {
        capacity: 400_000,
        prune_start_threshold: Some(2_000),
        min_size_after_prune: None,
    };
    let suffix: Suffix<MessengerCandidate> = Suffix::with_config(suffix_config);

    let inbound_service = MessengerInboundService {
        message_receiver_abcast: kafka_consumer,
        tx_actions_channel,
        rx_feedback_channel,
        suffix,
    };

    // TODO: GK - create topic should be part of publish.
    kafka_config.topic = "test.messenger.topic".to_string();
    let kafka_producer = KafkaProducer::new(&kafka_config);
    let messenger_kafka_publisher = MessengerKafkaPublisher { publisher: kafka_producer };

    let publish_service = PublishActionService {
        publisher: messenger_kafka_publisher,
        rx_actions_channel,
        tx_feedback_channel,
    };

    // inbound_service.run().await.unwrap();

    let messenger_service = TalosMessengerService {
        services: vec![Box::new(inbound_service), Box::new(publish_service)],
    };

    messenger_service.run().await.unwrap();
}
