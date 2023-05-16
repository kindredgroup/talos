// $coverage:ignore-start

use cohort::{
    config_loader::ConfigLoader,
    replicator::{
        core::{Replicator, ReplicatorCandidate},
        pg_replicator_installer::PgReplicatorStatemapInstaller,
        replicator_service::run_talos_replicator,
    },
    state::postgres::{data_access::PostgresApi, database::Database},
};
use log::info;
use talos_certifier::ports::MessageReciever;
use talos_certifier_adapters::{KafkaConfig, KafkaConsumer};
use talos_suffix::{core::SuffixConfig, Suffix};

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
        prune_start_threshold: Some(2000),
        min_size_after_prune: None,
    };
    let suffix: Suffix<ReplicatorCandidate> = Suffix::with_config(suffix_config);

    let mut replicator = Replicator::new(kafka_consumer, suffix);
    info!("Replicator starting...");

    let cfg_db = ConfigLoader::load_db_config().unwrap();
    let database = Database::init_db(cfg_db).await;
    let manual_tx_api = PostgresApi { client: database.get().await };

    let mut pg_statemap_installer = PgReplicatorStatemapInstaller { pg: manual_tx_api };

    run_talos_replicator(&mut replicator, &mut pg_statemap_installer).await;
}
// $coverage:ignore-end
