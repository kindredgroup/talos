// $coverage:ignore-start
use cohort::config_loader::ConfigLoader;
use cohort::core::Cohort;
use cohort::executors::threaded_csv::ThreadedCsvExecutor;
use cohort::replicator::core::{Replicator, ReplicatorCandidate};
use cohort::replicator::pg_replicator_installer::PgReplicatorStatemapInstaller;
use cohort::replicator::services::replicator_service::replicator_service;
use cohort::replicator::services::statemap_installer_service::installer_service;
use cohort::state::postgres::data_access::PostgresApi;
use cohort::state::postgres::database::Database;

use log::{info, warn};

use std::env;
use std::sync::Arc;

use talos_certifier::ports::MessageReciever;
use talos_certifier_adapters::{KafkaConfig, KafkaConsumer};
use talos_suffix::core::SuffixConfig;
use talos_suffix::Suffix;

use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::{signal, try_join};

pub type ServiceHandle = JoinHandle<Result<(), String>>;

struct LaunchParams {
    transactions_csv: String,
    threads: u8,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::builder().format_timestamp_millis().init();

    let params = get_params().await?;

    let (cfg_agent, cfg_kafka, cfg_db) = ConfigLoader::load()?;

    let database = Database::init_db(cfg_db.clone()).await;

    let (h_replicator, h_installer) = start_replicator(Arc::clone(&database)).await;

    let agent = Arc::new(Cohort::init_agent(cfg_agent, cfg_kafka).await);

    let h_cohort = tokio::spawn(async move { ThreadedCsvExecutor::start(agent, database, params.transactions_csv, params.threads).await });

    let all_async_services = tokio::spawn(async move {
        // g. Run the 2 services.
        let result = try_join!(h_replicator, h_installer, h_cohort);
        // h. Both the services are in infinite loops.
        //    We reach here only if there was an error in either of the service.
        warn!("Result from the services ={result:?}");

        result.unwrap()
    });

    tokio::select! {
        _ = all_async_services => {}

        // CTRL + C termination signal
        _ = signal::ctrl_c() => {
            log::info!("Shutting down");
        }
    }

    info!("Exiting example app \"Cohort with replicator\"");

    Ok(())
}

async fn start_replicator(database: Arc<Database>) -> (ServiceHandle, ServiceHandle) {
    let channel_size = 3_000;
    let (replicator_tx, replicator_rx) = mpsc::channel(channel_size);
    let (statemap_installer_tx, statemap_installer_rx) = mpsc::channel(channel_size);

    let suffix_config = SuffixConfig {
        capacity: 10,
        prune_start_threshold: Some(2000),
        min_size_after_prune: None,
    };

    let suffix: Suffix<ReplicatorCandidate> = Suffix::with_config(suffix_config);

    let mut kafka_config = KafkaConfig::from_env();
    kafka_config.group_id = "talos-replicator-dev".to_string();
    let kafka_consumer = KafkaConsumer::new(&kafka_config);

    // b. Subscribe to topic.
    kafka_consumer.subscribe().await.unwrap();

    let replicator = Replicator::new(kafka_consumer, suffix);
    let replicator_service = replicator_service(statemap_installer_tx, replicator_rx, replicator);

    let replicator_handle = tokio::spawn(async move { replicator_service.await });

    let manual_tx_api = PostgresApi { client: database.get().await };
    let pg_statemap_installer = PgReplicatorStatemapInstaller { pg: manual_tx_api };
    let pg_statemap_installer_service = installer_service(statemap_installer_rx, replicator_tx, pg_statemap_installer);
    let installer_handle = tokio::spawn(async move { pg_statemap_installer_service.await });

    (replicator_handle, installer_handle)
}

async fn get_params() -> Result<LaunchParams, String> {
    let args: Vec<String> = env::args().collect();
    let mut transactions: Option<String> = None;
    let mut threads: Option<u8> = Some(1);

    if args.len() >= 3 {
        let mut i = 1;
        while i < args.len() {
            let param_name = &args[i];

            if param_name.eq("--transactions") {
                let param_value = &args[i + 1];
                let mut file = File::open(param_value).await.unwrap();
                let mut content = String::from("");
                let _ = file.read_to_string(&mut content).await;
                transactions = Some(content.replace(['\t', ' '], ""));
            } else if param_name.eq("--threads") {
                let param_value = &args[i + 1];
                threads = Some(param_value.parse().unwrap());
            }

            i += 2;
        }
    }

    if transactions.is_none() {
        Err("Parameter --transactions is required".into())
    } else {
        Ok(LaunchParams {
            transactions_csv: transactions.unwrap(),
            threads: threads.unwrap(),
        })
    }
}

// $coverage:ignore-end
