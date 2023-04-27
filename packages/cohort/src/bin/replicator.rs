use std::{io::Error, sync::Arc, vec};

use cohort::{
    bank_api::Transfer,
    config_loader::ConfigLoader,
    replicator::{
        core::{Replicator, ReplicatorCandidate, StatemapItem},
        replicator_service::run_talos_replicator,
    },
    state::{model::AccountRef, postgres::database::Database},
};
use log::info;
use serde::{Deserialize, Serialize};
use talos_certifier::ports::MessageReciever;
use talos_certifier_adapters::{KafkaConfig, KafkaConsumer};
use talos_suffix::{core::SuffixConfig, Suffix};

#[derive(Debug, Serialize, Deserialize)]
struct ReplicatorBankTransfer {
    from: String,
    to: String,
    amount: f64,
}

async fn statemap_install_handler(sm: Vec<StatemapItem>, db: Arc<Database>) -> Result<bool, Error> {
    info!("Original statemaps received ... {:#?} ", sm);

    // Convert the StatemapItems to the one understood by BankAPI.
    let bank_batch = sm.into_iter().fold(vec![], |mut acc, sm_item| {
        if sm_item.action == "bank_transfer" {
            let new_version = sm_item.version;
            let payload = serde_json::from_value::<ReplicatorBankTransfer>(sm_item.payload).unwrap();
            acc.push(Transfer::new(
                AccountRef::new(payload.from, Some(new_version)),
                AccountRef::new(payload.to, Some(new_version)),
                payload.amount.to_string(),
                new_version,
                true,
            ));
        }

        acc
    });

    // Filter out the statemap actions not applicable to this Cohort.
    info!("The database pool is ={:?}", db.pool.get().await);
    info!("Bank Api batch ... {:#?} ", bank_batch);
    // Execute the batch including updating the snapshot.
    let db_result = db.batch(bank_batch, None).await;
    info!("Database operation result ={:?}", db_result);
    Ok(true)
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

    let cfg_db = ConfigLoader::load_db_config().unwrap();
    let database = Database::init_db(cfg_db).await;

    let installer_callback = |sm: Vec<StatemapItem>| async {
        // call the statemap installer.
        statemap_install_handler(sm, Arc::clone(&database)).await
    };

    run_talos_replicator(&mut replicator, installer_callback).await;
}
