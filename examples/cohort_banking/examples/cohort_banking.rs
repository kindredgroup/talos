use std::{env, sync::Arc, time::Duration};

use async_channel::Receiver;
use cohort::{
    config_loader::ConfigLoader,
    metrics::Stats,
    model::requests::TransferRequest,
    replicator::{
        core::{Replicator, ReplicatorCandidate},
        pg_replicator_installer::PgReplicatorStatemapInstaller,
        services::{replicator_service::replicator_service, statemap_installer_service::installation_service, statemap_queue_service::statemap_queue_service},
        utils::installer_utils::get_snapshot_callback,
    },
    snapshot_api::SnapshotApi,
    state::postgres::database::Database,
};
use examples_support::{
    cohort::queue_workers::QueueProcessor,
    load_generator::{generator::ControlledRateLoadGenerator, models::StopType},
};

use metrics::model::{MicroMetrics, MinMax};
use rand::Rng;
use rust_decimal::{prelude::FromPrimitive, Decimal};
use talos_certifier::ports::MessageReciever;
use talos_certifier_adapters::{KafkaConfig, KafkaConsumer};
use talos_suffix::{core::SuffixConfig, Suffix};
use tokio::{signal, sync::Mutex, task::JoinHandle, try_join};

type ReplicatorTaskHandle = JoinHandle<Result<(), String>>;
type InstallerQueuTaskHandle = JoinHandle<Result<(), String>>;
type InstallationTaskHandle = JoinHandle<Result<(), String>>;
type HeartBeatReceiver = tokio::sync::watch::Receiver<u64>;

#[derive(Clone)]
struct LaunchParams {
    stop_type: StopType,
    target_rate: f32,
    threads: u64,
    max_retry: u64,
    replicator_metrics: Option<i128>,
    cohort_metrics: Option<i128>,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::builder().format_timestamp_millis().init();

    log::info!("Started, pid: {}", std::process::id());

    let params = get_params().await?;

    let (tx_queue, rx_queue) = async_channel::unbounded::<TransferRequest>();
    let rx_queue = Arc::new(rx_queue);
    let rx_queue_ref = Arc::clone(&rx_queue);

    let generator = ControlledRateLoadGenerator::generate(params.stop_type, params.target_rate, &create_transfer_request, Arc::new(tx_queue));

    let h_generator = tokio::spawn(generator);
    let (tx_metrics, rx_metrics) = async_channel::unbounded::<Stats>();
    let tx_metrics = Arc::new(tx_metrics);
    let rx_metrics = Arc::new(rx_metrics);
    let rx_metrics_ref1 = Arc::clone(&rx_metrics);
    let rx_metrics_ref2 = Arc::clone(&rx_metrics);

    let cfg_db = ConfigLoader::load_db_config()?;
    let db = Database::init_db(cfg_db).await;
    let db_ref1 = Arc::clone(&db);
    let db_ref2 = Arc::clone(&db);

    let h_cohort = tokio::spawn(
        // pass tasks queue "rx_generated" to be accessed by multiple worker threads
        async move { QueueProcessor::process(rx_queue, tx_metrics, params.threads, params.max_retry, db_ref1, params.cohort_metrics).await },
    );

    // TODO: extract 100_000 into command line parameter - channel_size between replicator and installer tasks
    let (h_replicator, h_installer, h_installation, rx_heartbeat) = start_replicator(params.replicator_metrics, db_ref2, 100_000).await;

    let metrics_data = Arc::new(Mutex::new(Vec::new()));
    let metrics_data = Arc::clone(&metrics_data);
    let h_metrics_collector = start_metrics_collector::<Stats>(rx_metrics_ref1, Arc::clone(&metrics_data));

    let h_stop: JoinHandle<Result<(), String>> = start_queue_monitor(
        rx_queue_ref,
        rx_heartbeat,
        // once queue is empty we close metrics channel, which will cause metrics collector thread to finish
        move || rx_metrics_ref2.close(),
    );

    let all_async_services = tokio::spawn(async move {
        let result = try_join!(h_generator, h_replicator, h_installer, h_installation, h_cohort, h_metrics_collector);
        log::warn!("Result from the services ={result:?}");
    });

    tokio::select! {
        _ = h_stop => {
            log::warn!("Stop manager is active...");
        }

        _ = all_async_services => {}

        // CTRL + C termination signal
        _ = signal::ctrl_c() => {
            log::warn!("Shutting down...");
        }
    }

    log::warn!("The processing has finished, computing stats....");

    let data = metrics_data.lock().await;
    let mut stats = Stats::default();
    for item in data.iter() {
        stats.merge(item.clone());
    }

    log::warn!("{}", stats.generate_report(params.threads, params.max_retry));

    Ok(())
}

// TODO: Fix and enable these lints
#[allow(unused_variables, unused_mut)]
fn start_queue_monitor(
    queue: Arc<Receiver<TransferRequest>>,
    mut rx_heartbeat: tokio::sync::watch::Receiver<u64>,
    fn_on_empty_queue: impl FnOnce() -> bool + Send + 'static,
) -> JoinHandle<Result<(), String>> {
    tokio::spawn(async move {
        let check_frequency = Duration::from_secs(10);
        let total_attempts = 3;

        let mut remaining_attempts = total_attempts;
        loop {
            if remaining_attempts == 0 {
                // we consumed all attempts
                break;
            }
            // check heartbeat channel, are there any updates coming from other workiers?
            let (recent_heartbeat_value, is_count_changed) = {
                let reference = rx_heartbeat.borrow_and_update();
                (*reference, reference.has_changed())
            };

            // if queue.is_empty() && !is_count_changed {
            //     // queue is empty and there are no signals from other workers, reduce window and try again
            //     remaining_attempts -= 1;
            //     log::warn!(
            //         "Workers queue is empty and there is no activity signal from replicator. Finishing in: {} seconds...",
            //         remaining_attempts * check_frequency.as_secs()
            //     );
            // } else {
            //     remaining_attempts = total_attempts;
            //     log::warn!(
            //         "Counts. Remaining: {}, processed by replicator and installer: {}",
            //         queue.len(),
            //         recent_heartbeat_value
            //     );
            // }

            tokio::time::sleep(check_frequency).await;
        }

        queue.close();
        fn_on_empty_queue();

        Err("Signal from StopController".into())
    })
}

fn start_metrics_collector<T: Send + 'static>(metrics_source: Arc<Receiver<T>>, metrics_sink: Arc<Mutex<Vec<T>>>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match metrics_source.recv().await {
                Err(_) => break,
                Ok(stats) => {
                    metrics_sink.lock().await.push(stats);
                }
            }
        }
    })
}

async fn start_replicator(
    replicator_metrics: Option<i128>,
    database: Arc<Database>,
    channel_size: usize,
) -> (ReplicatorTaskHandle, InstallerQueuTaskHandle, InstallationTaskHandle, HeartBeatReceiver) {
    let mut kafka_config = KafkaConfig::from_env();
    kafka_config.group_id = "talos-replicator-dev".to_string();
    let kafka_consumer = KafkaConsumer::new(&kafka_config);

    // b. Subscribe to topic.
    kafka_consumer.subscribe().await.unwrap();

    let (_tx_heartbeat, rx_heartbeat) = tokio::sync::watch::channel(0_u64);
    let (tx_install_req, rx_install_req) = tokio::sync::mpsc::channel(channel_size);
    let (tx_install_resp, rx_install_resp) = tokio::sync::mpsc::channel(channel_size);

    let (tx_installation_feedback_req, rx_installation_feedback_req) = tokio::sync::mpsc::channel(channel_size);
    let (tx_installation_req, rx_installation_req) = tokio::sync::mpsc::channel(channel_size);
    // let manual_tx_api = PostgresApi { client: database.get().await };
    let installer = PgReplicatorStatemapInstaller {
        metrics_frequency: replicator_metrics,
        pg: database.clone(),
        metrics: MicroMetrics::new(1_000_000_000_f32, true),
        m_total: MinMax::default(),
        m1_tx: MinMax::default(),
        m2_exec: MinMax::default(),
        m3_ver: MinMax::default(),
        m4_snap: MinMax::default(),
        m5_commit: MinMax::default(),
    };

    let suffix_config = SuffixConfig {
        capacity: 10,
        prune_start_threshold: Some(2000),
        min_size_after_prune: None,
    };
    let talos_suffix: Suffix<ReplicatorCandidate> = Suffix::with_config(suffix_config);
    let replicator_v1 = Replicator::new(kafka_consumer, talos_suffix);
    let future_replicator = replicator_service(tx_install_req, rx_install_resp, replicator_v1);
    // let future_installer = installer_service(rx_install_req, tx_install_resp, installer);

    let get_snapshot_fn = get_snapshot_callback(SnapshotApi::query(database.clone()));
    let future_installer_queue = statemap_queue_service(rx_install_req, rx_installation_feedback_req, tx_installation_req, get_snapshot_fn);
    let future_installation = installation_service(tx_install_resp, Arc::new(installer), rx_installation_req, tx_installation_feedback_req);

    let h_replicator = tokio::spawn(future_replicator);
    let h_installer = tokio::spawn(future_installer_queue);
    let h_installation = tokio::spawn(future_installation);

    (h_replicator, h_installer, h_installation, rx_heartbeat)
}

fn create_transfer_request() -> TransferRequest {
    let mut available_accounts = 0_u64;
    let args: Vec<String> = env::args().collect();
    if args.len() >= 2 {
        let mut i = 1;
        while i < args.len() {
            let param_name = &args[i];
            if param_name.eq("--accounts") {
                let param_value = &args[i + 1];
                available_accounts = param_value.parse().unwrap();
            }
            i += 2;
        }
    }

    let mut rnd = rand::thread_rng();
    let mut to;

    let from = rnd.gen_range(1..=available_accounts);
    loop {
        to = rnd.gen_range(1..=available_accounts);
        if to == from {
            continue;
        }
        break;
    }

    TransferRequest {
        from: format!("{:<04}", from),
        to: format!("{:<04}", to),
        amount: Decimal::from_f32(1.0).unwrap(),
    }
}

async fn get_params() -> Result<LaunchParams, String> {
    let args: Vec<String> = env::args().collect();
    let mut threads: Option<u64> = Some(1);
    let mut max_retry: Option<u64> = Some(10);
    let mut accounts: Option<u64> = None;
    let mut target_rate: Option<f32> = None;
    let mut stop_type: Option<StopType> = None;
    let mut replicator_metrics: Option<i128> = None;
    let mut cohort_metrics: Option<i128> = None;

    if args.len() >= 3 {
        let mut i = 1;
        while i < args.len() {
            let param_name = &args[i];
            if param_name.eq("--accounts") {
                let param_value = &args[i + 1];
                accounts = Some(param_value.parse().unwrap());
            } else if param_name.eq("--threads") {
                let param_value = &args[i + 1];
                threads = Some(param_value.parse().unwrap());
            } else if param_name.eq("--max-retry") {
                let param_value = &args[i + 1];
                max_retry = Some(param_value.parse().unwrap());
            } else if param_name.eq("--rate") {
                let param_value = &args[i + 1];
                target_rate = Some(param_value.parse().unwrap());
            } else if param_name.eq("--volume") {
                let param_value = &args[i + 1];

                if param_value.contains("-sec") {
                    let seconds: u64 = param_value.replace("-sec", "").parse().unwrap();
                    stop_type = Some(StopType::LimitExecutionDuration {
                        run_duration: Duration::from_secs(seconds),
                    })
                } else {
                    let count: u64 = param_value.parse().unwrap();
                    stop_type = Some(StopType::LimitGeneratedTransactions { count })
                }
            } else if param_name.eq("--replicator-metrics") {
                let param_value = &args[i + 1];
                if param_value.contains("-sec") {
                    let seconds: u64 = param_value.replace("-sec", "").parse().unwrap();
                    replicator_metrics = Some(Duration::from_secs(seconds).as_nanos() as i128);
                } else {
                    replicator_metrics = Some(Duration::from_secs(param_value.parse().unwrap()).as_nanos() as i128);
                }
            } else if param_name.eq("--cohort-metrics") {
                let param_value = &args[i + 1];
                if param_value.contains("-sec") {
                    let seconds: u64 = param_value.replace("-sec", "").parse().unwrap();
                    cohort_metrics = Some(Duration::from_secs(seconds).as_nanos() as i128);
                } else {
                    cohort_metrics = Some(Duration::from_secs(param_value.parse().unwrap()).as_nanos() as i128);
                }
            }

            i += 2;
        }
    }

    if stop_type.is_none() {
        Err("Parameter --volume is required".into())
    } else if accounts.is_none() {
        Err("Parameter --accounts is required".into())
    } else if target_rate.is_none() {
        Err("Parameter --rate is required".into())
    } else {
        Ok(LaunchParams {
            target_rate: target_rate.unwrap(),
            stop_type: stop_type.unwrap(),
            threads: threads.unwrap(),
            max_retry: max_retry.unwrap(),
            replicator_metrics,
            cohort_metrics,
        })
    }
}
