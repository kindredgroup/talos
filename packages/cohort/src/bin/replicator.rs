// $coverage:ignore-start
use std::{io::Error, sync::Arc};

use cohort::{
    config_loader::ConfigLoader,
    replicator::{
        core::{Replicator, ReplicatorCandidate, StatemapItem},
        replicator_service::run_talos_replicator,
    },
    state::postgres::database::Database,
    tx_batch_executor::BatchExecutor,
};
use log::info;
use talos_certifier::ports::MessageReciever;
use talos_certifier_adapters::{KafkaConfig, KafkaConsumer};
use talos_suffix::{core::SuffixConfig, Suffix};

async fn statemap_install_handler(sm: Vec<StatemapItem>, db: Arc<Database>) -> Result<bool, Error> {
    info!("Original statemaps received ... {:#?} ", sm);

    let result = BatchExecutor::execute(&db, sm, None).await;

    info!("Result on executing the statmaps is ... {result:?}");

    Ok(result.is_ok())
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
        prune_start_threshold: Some(2),
        min_size_after_prune: None,
    };
    let suffix: Suffix<ReplicatorCandidate> = Suffix::with_config(suffix_config);

    let mut replicator = Replicator::new(kafka_consumer, suffix);
    info!("Replicator starting...");

    let cfg_db = ConfigLoader::load_db_config().unwrap();
    let database = Database::init_db(cfg_db).await;

    let installer_callback = |sm: Vec<StatemapItem>| async {
        // call the statemap installer.
        statemap_install_handler(sm, Arc::clone(&database)).await
    };

    run_talos_replicator(&mut replicator, installer_callback).await;
}
// $coverage:ignore-end
