// $coverage:ignore-start

use std::sync::Arc;

use cohort::{
    config_loader::ConfigLoader,
    replicator::{
        core::Replicator,
        models::ReplicatorCandidate,
        pg_replicator_installer::PgReplicatorStatemapInstaller,
        services::{
            replicator_service::{replicator_service, ReplicatorServiceConfig},
            statemap_installer_service::{installation_service, StatemapInstallerConfig},
            statemap_queue_service::{statemap_queue_service, StatemapQueueServiceConfig},
        },
        utils::get_snapshot_callback,
    },
    snapshot_api::SnapshotApi,
    state::postgres::database::Database,
};
use log::{info, warn};
use metrics::model::{MicroMetrics, MinMax};
use talos_certifier::{env_var_with_defaults, ports::MessageReciever};
use talos_certifier_adapters::{KafkaConfig, KafkaConsumer};
use talos_suffix::{core::SuffixConfig, Suffix};
use tokio::{signal, sync::mpsc, try_join};

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::builder().format_timestamp_millis().init();

    // 0. Create required items.
    //  a. Create Kafka consumer
    let mut kafka_config = KafkaConfig::from_env();
    kafka_config.group_id = "talos-replicator-dev-1".to_string();
    let kafka_consumer = KafkaConsumer::new(&kafka_config);

    // b. Subscribe to topic.
    kafka_consumer.subscribe().await.unwrap();

    //  c. Create suffix.
    let replicator_config = ReplicatorServiceConfig {
        commit_frequency_ms: env_var_with_defaults!("REPLICATOR_KAFKA_COMMIT_FREQ_MS", u64, 10_000),
        enable_stats: env_var_with_defaults!("REPLICATOR_ENABLE_STATS", bool, true),
    };
    let suffix_config = SuffixConfig {
        capacity: env_var_with_defaults!("REPLICATOR_SUFFIX_CAPACITY", usize, 100_000),
        prune_start_threshold: env_var_with_defaults!("REPLICATOR_SUFFIX_PRUNE_THRESHOLD", Option::<usize>, 2_000),
        min_size_after_prune: env_var_with_defaults!("REPLICATOR_SUFFIX_MIN_SIZE", Option::<usize>),
    };
    let suffix: Suffix<ReplicatorCandidate> = Suffix::with_config(suffix_config);

    //  d. Intantiate the replicator.
    let replicator = Replicator::new(kafka_consumer, suffix);
    info!("Replicator starting...");

    // e. Create postgres statemap installer instance.
    let cfg_db = ConfigLoader::load_db_config().unwrap();
    let database = Database::init_db(cfg_db).await.map_err(|e| e.to_string())?;

    let pg_statemap_installer = PgReplicatorStatemapInstaller {
        metrics_frequency: None,
        pg: database.clone(),
        metrics: MicroMetrics::new(1_000_000_000_f32, true),
        m_total: MinMax::default(),
        m1_tx: MinMax::default(),
        m2_exec: MinMax::default(),
        m3_ver: MinMax::default(),
        m4_snap: MinMax::default(),
        m5_commit: MinMax::default(),
    };

    // f. Create the replicator and statemap installer services.
    // run_talos_cohort_replicator(&mut replicator, &mut pg_statemap_installer).await;
    let (replicator_tx, replicator_rx) = mpsc::channel(3_000);
    let (statemap_installer_tx, statemap_installer_rx) = mpsc::channel(3_000);
    let (tx_installation_req, rx_installation_req) = mpsc::channel(3_000);
    let (tx_installation_feedback_req, rx_installation_feedback_req) = mpsc::channel(3_000);

    let replicator_service = replicator_service(statemap_installer_tx, replicator_rx, replicator, replicator_config);

    let get_snapshot_fn = get_snapshot_callback(SnapshotApi::query(database.clone()));
    let enable_stats = env_var_with_defaults!("COHORT_SQ_ENABLE_STATS", bool, true);
    let queue_cleanup_frequency_ms = env_var_with_defaults!("COHORT_SQ_QUEUE_CLEANUP_FREQUENCY_MS", u64, 10_000);
    let queue_config = StatemapQueueServiceConfig {
        enable_stats,
        queue_cleanup_frequency_ms,
    };
    let future_installer_queue = statemap_queue_service(
        statemap_installer_rx,
        rx_installation_feedback_req,
        tx_installation_req,
        get_snapshot_fn,
        queue_config,
    );

    let thread_pool = env_var_with_defaults!("COHORT_INSTALLER_PARALLEL_COUNT", Option::<u16>, 50);
    let installer_config = StatemapInstallerConfig { thread_pool };
    let future_installation = installation_service(
        replicator_tx,
        Arc::new(pg_statemap_installer),
        rx_installation_req,
        tx_installation_feedback_req,
        installer_config,
    );

    let replicator_handle = tokio::spawn(replicator_service);
    let h_installer = tokio::spawn(future_installer_queue);
    let h_installation = tokio::spawn(future_installation);

    let handle = tokio::spawn(async move {
        // g. Run the services.
        let result = try_join!(replicator_handle, h_installer, h_installation);
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

    Ok(())
}
// $coverage:ignore-end
