// $coverage:ignore-start

use std::{collections::HashMap, sync::Arc};

use cohort::{
    config_loader::ConfigLoader,
    replicator::{
        core::{Replicator, ReplicatorCandidate},
        pg_replicator_installer::PgReplicatorStatemapInstaller,
        services::{replicator_service::replicator_service, statemap_installer_service::installer_service, stats_service::stats_service},
        statistics::{core::ReplicatorStatisticsItem, utils::generate_statistics},
    },
    state::postgres::{data_access::PostgresApi, database::Database},
};
use log::{info, warn};
use talos_certifier::ports::MessageReciever;
use talos_certifier_adapters::{KafkaConfig, KafkaConsumer};
use talos_suffix::{core::SuffixConfig, Suffix};
use tokio::{
    signal,
    sync::{mpsc, Mutex},
    try_join,
};

#[tokio::main]
async fn main() {
    env_logger::builder().format_timestamp_millis().init();

    let stats_object: HashMap<u64, ReplicatorStatisticsItem> = HashMap::new();
    let stats = Arc::new(Mutex::new(stats_object));

    let option_collect_stats = option_env!("REPLICATOR_ENABLE_STATS");
    let should_collect_stats = if option_collect_stats.is_some() {
        option_collect_stats.unwrap().parse::<bool>().unwrap_or_default()
    } else {
        false
    };
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

    //  d. Intantiate the replicator.
    let replicator = Replicator::new(kafka_consumer, suffix);
    info!("Replicator starting...");

    // e. Create postgres statemap installer instance.
    let cfg_db = ConfigLoader::load_db_config().unwrap();
    let database = Database::init_db(cfg_db).await;
    let manual_tx_api = PostgresApi { client: database.get().await };

    let pg_statemap_installer = PgReplicatorStatemapInstaller { pg: manual_tx_api };

    // f. Create the replicator and statemap installer services.
    // run_talos_replicator(&mut replicator, &mut pg_statemap_installer).await;
    let (replicator_tx, replicator_rx) = mpsc::channel(3_000);
    let (statemap_installer_tx, statemap_installer_rx) = mpsc::channel(3_000);
    let (statistics_tx, statistics_rx) = mpsc::channel(3_000);

    // statemap installer service.
    let stats_tx = if should_collect_stats { Some(statistics_tx.clone()) } else { None };
    let pg_statemap_installer_service = installer_service(statemap_installer_rx, replicator_tx, pg_statemap_installer, stats_tx);
    let installer_handle = tokio::spawn(async move { pg_statemap_installer_service.await });

    // replicator service.
    let stats_tx = if should_collect_stats { Some(statistics_tx.clone()) } else { None };
    let replicator_service = replicator_service(statemap_installer_tx, replicator_rx, replicator, stats_tx);
    let replicator_handle = tokio::spawn(async move { replicator_service.await });

    // statistics service.
    let statistics_service = stats_service(Arc::clone(&stats), statistics_rx, should_collect_stats);
    let statistics_handle = tokio::spawn(async move { statistics_service.await });

    let handle = tokio::spawn(async move {
        // g. Run the 2 services.
        let result = try_join!(replicator_handle, installer_handle, statistics_handle);
        // h. Both the services are in infinite loops.
        //    We reach here only if there was an error in either of the service.
        warn!("Result from the services ={result:?}");

        result.unwrap()
    });

    tokio::select! {
        _ = handle => {}
        // CTRL + C termination signal
        _ = signal::ctrl_c() => {
            log::info!("CTRL + C TERMINATION!!!!");
        }
    }

    let s = stats.lock().await;
    info!("Stats Object ={s:#?}");
    drop(s);

    if should_collect_stats {
        generate_statistics(stats).await;
    }

    info!("Exiting Cohort Replicator!!");
}
// $coverage:ignore-end
