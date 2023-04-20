use cohort::replicator::{
    core::{ReplicatorCandidate, StatemapItem},
    replicator_service::{run_talos_replicator, Replicator},
};
use log::info;
use talos_certifier::ports::MessageReciever;
use talos_certifier_adapters::{KafkaConfig, KafkaConsumer};
use talos_suffix::{core::SuffixConfig, Suffix};

fn statemap_install_handler(sm: Vec<StatemapItem>) -> bool {
    info!("Printing the length of statemaps ... {}", sm.len());
    true
}

#[tokio::main]
async fn main() {
    env_logger::builder().format_timestamp_millis().init();

    // 0. Create required items.
    //  a. Create Kafka consumer
    let mut kafka_config = KafkaConfig::from_env();
    kafka_config.group_id = "talos-replicator-dev".to_string();
    let kafka_consumer = KafkaConsumer::new(&kafka_config);

    // b. Subscribe to topic.
    kafka_consumer.subscribe().await.unwrap();

    //  c. Create suffix.
    let suffix_config = SuffixConfig {
        capacity: 10,
        prune_start_threshold: None,
        min_size_after_prune: None,
    };
    let suffix: Suffix<ReplicatorCandidate> = Suffix::with_config(suffix_config);

    let mut replicator = Replicator::new(kafka_consumer, suffix);
    info!("Replicator starting...");

    run_talos_replicator(&mut replicator, statemap_install_handler).await;
}
