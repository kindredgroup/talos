use talos_common_utils::env_var;
use talos_messenger_actions::messenger_with_kafka::{messenger_with_kafka, Configuration};
use talos_messenger_core::utlis::{create_whitelist_actions_from_str, ActionsParserConfig};
use talos_rdkafka_utils::kafka_config::KafkaConfig;
use talos_suffix::core::SuffixConfig;

#[tokio::main]
async fn main() {
    env_logger::builder().format_timestamp_millis().init();

    // 0. Create required items.
    //  a. Create Kafka consumer
    let mut kafka_config = KafkaConfig::from_env(None);
    // kafka_config.group_id = env_var!("TALOS_MESSENGER_KAFKA_GROUP_ID");
    kafka_config.extend(
        None,
        Some(
            [
                ("group.id".to_string(), env_var!("TALOS_MESSENGER_KAFKA_GROUP_ID")),
                ("enable.auto.commit".to_string(), "false".to_string()),
                ("auto.offset.reset".to_string(), "earliest".to_string()),
            ]
            .into(),
        ),
    );

    let suffix_config = SuffixConfig {
        capacity: 400_000,
        prune_start_threshold: Some(3_000),
        min_size_after_prune: Some(2_000),
    };

    let actions_from_env = env_var!("TALOS_MESSENGER_ACTIONS_WHITELIST");
    let allowed_actions = create_whitelist_actions_from_str(&actions_from_env, &ActionsParserConfig::default());

    let config = Configuration {
        suffix_config: Some(suffix_config),
        kafka_config,
        allowed_actions,
        channel_buffers: None,
    };

    messenger_with_kafka(config).await.unwrap();
}
