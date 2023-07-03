// $coverage:ignore-start

use cohort::{
    config_loader::ConfigLoader,
    replicator::{
        core::{Replicator, ReplicatorCandidate},
        pg_replicator_installer::PgReplicatorStatemapInstaller,
        services::{replicator_service::replicator_service, statemap_installer_service::installer_service},
    },
    state::postgres::{data_access::PostgresApi, database::Database},
};
use log::{info, warn};
use metrics::model::{MicroMetrics, MinMax};
use talos_certifier::ports::MessageReciever;
use talos_certifier_adapters::{KafkaConfig, KafkaConsumer};
use talos_suffix::{core::SuffixConfig, Suffix};
use tokio::{signal, sync::mpsc, try_join};

#[tokio::main]
async fn main() {
    env_logger::builder().format_timestamp_millis().init();

    // 0. Create required items.
    //  a. Create Kafka consumer
    let mut kafka_config = KafkaConfig::from_env();
    kafka_config.group_id = "talos-replicator-dev-1".to_string();
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

    let pg_statemap_installer = PgReplicatorStatemapInstaller {
        metrics_frequency: None,
        pg: manual_tx_api,
        metrics: MicroMetrics::new(1_000_000_000_f32, true),
        m_total: MinMax::default(),
        m1_tx: MinMax::default(),
        m2_exec: MinMax::default(),
        m3_ver: MinMax::default(),
        m4_snap: MinMax::default(),
        m5_commit: MinMax::default(),
    };

    // f. Create the replicator and statemap installer services.
    // run_talos_replicator(&mut replicator, &mut pg_statemap_installer).await;
    let (replicator_tx, replicator_rx) = mpsc::channel(3_000);
    let (statemap_installer_tx, statemap_installer_rx) = mpsc::channel(3_000);

    let pg_statemap_installer_service = installer_service(statemap_installer_rx, replicator_tx, pg_statemap_installer);

    let replicator_service = replicator_service(statemap_installer_tx, replicator_rx, replicator);

    let replicator_handle = tokio::spawn(replicator_service);
    let installer_handle = tokio::spawn(pg_statemap_installer_service);

    let handle = tokio::spawn(async move {
        // g. Run the 2 services.
        let result = try_join!(replicator_handle, installer_handle,);
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

    info!("Exiting Cohort Replicator!!");
}
// $coverage:ignore-end
