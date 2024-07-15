use std::{collections::HashMap, env, sync::Arc, time::Duration};

use async_channel::Receiver;
use banking_common::model::TransferRequest;
use banking_common::state::postgres::database_config::DatabaseConfig;
use cohort_banking::app::BankingApp;
use cohort_banking::examples_support::queue_processor::QueueProcessor;
use cohort_sdk::model::{BackoffConfig, Config};
use examples_support::load_generator::models::Generator;
use examples_support::load_generator::{generator::ControlledRateLoadGenerator, models::StopType};

use opentelemetry_api::global;
use opentelemetry_api::metrics::MetricsError;
use opentelemetry_sdk::metrics::reader::DefaultTemporalitySelector;
use opentelemetry_sdk::metrics::{MeterProvider, PeriodicReader};
use opentelemetry_sdk::runtime;
use opentelemetry_stdout::MetricsExporterBuilder;
use rand::Rng;
use rust_decimal::prelude::FromPrimitive;
use talos_metrics::opentel::aggregation_selector::CustomHistogramSelector;
use talos_metrics::opentel::printer::MetricsToStringPrinter;
use talos_metrics::opentel::scaling::ScalingConfig;
use talos_rdkafka_utils::kafka_config::KafkaConfig;
use tokio::{signal, task::JoinHandle, try_join};

use opentelemetry::global::shutdown_tracer_provider;

#[derive(Clone)]
struct LaunchParams {
    stop_type: StopType,
    target_rate: f32,
    threads: u64,
    accounts: u64,
    scaling_config: HashMap<String, f32>,
    metric_print_raw: bool,
}

/// Connects to database, to kafka certification topic as talos agent and as cohort replicator.
/// Generates some number of banking transactions and passes them all to Talos for certification.
/// Once all transactions have been processed, it prints some output metrics to console.
/// Metric logging is set to WARN. If RUST_LOG is set to stricter than WARN then no metrics will be printed.
/// Preprequisite:
///     Talos, Kafka and DB must be running
///     Kafka topic must be empty.
///     DB should have snapshot table initialised with zero version
///     Banking database should have some accounts ready.
///
/// Lauch parameters:
/// --accounts - How many accouts are avaiable in database.
/// --threads - How many threads to use.
/// --rate - In TPS, at what rate to generate transactions.
/// --volume - How many transaction to generate or "--volume 10-sec" for how long to generate transactions.
/// --metric_print_raw - When present, raw metric histograms will be printed. The value of this param is ignored.
/// --metric_scaling - The default scaling is 1.0; this parameter controls scaling factor for individual metric.
///                    Format is: "--metric_scaling metric-name1=scaling-factor,metric-name2=scaling-factor,..."
#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::builder().format_timestamp_millis().init();
    log::info!("Started, pid: {}", std::process::id());

    let params = get_params().await?;

    let (tx_queue, rx_queue) = async_channel::unbounded::<(TransferRequest, f64)>();
    let rx_queue = Arc::new(rx_queue);
    let rx_queue_ref = Arc::clone(&rx_queue);

    let generator_impl = TransferRequestGenerator {
        available_accounts: params.accounts,
        generated: Vec::new(),
    };

    let generator = ControlledRateLoadGenerator::generate(params.stop_type, params.target_rate, generator_impl, Arc::new(tx_queue));
    let h_generator = tokio::spawn(generator);

    let mut cfg_kafka = KafkaConfig::from_env(Some("COHORT"));
    let producer_config_overrides = [
        // The maximum time librdkafka may use to deliver a message (including retries)
        ("message.timeout.ms".to_string(), "15000".to_string()),
        ("queue.buffering.max.messages".to_string(), "1000000".to_string()),
        ("topic.metadata.refresh.interval.ms".to_string(), "5".to_string()),
        ("socket.keepalive.enable".to_string(), "true".to_string()),
        ("acks".to_string(), "0".to_string()),
    ];

    let consumer_config_overrides = [
        ("enable.auto.commit".to_string(), "false".to_string()),
        ("auto.offset.reset".to_string(), "latest".to_string()),
        ("fetch.wait.max.ms".to_string(), "600".to_string()),
        ("socket.keepalive.enable".to_string(), "true".to_string()),
        ("acks".to_string(), "0".to_string()),
    ];

    cfg_kafka.extend(Some(HashMap::from(producer_config_overrides)), Some(HashMap::from(consumer_config_overrides)));

    let sdk_config = Config {
        //
        // cohort configs
        //
        backoff_on_conflict: BackoffConfig::new(1, 1500),
        retry_backoff: BackoffConfig::new(20, 1500),
        retry_attempts_max: 10,
        retry_oo_backoff: BackoffConfig::new(20, 1000),
        retry_oo_attempts_max: 10,

        snapshot_wait_timeout_ms: 10_000,

        //
        // agent config values
        //
        agent: "cohort-banking".into(),
        cohort: "cohort-banking".into(),
        // The size of internal buffer for candidates
        buffer_size: 10_000_000,
        timeout_ms: 600_000,
        kafka: cfg_kafka,
    };

    let db_config = DatabaseConfig::from_env(Some("COHORT"))?;

    let printer = MetricsToStringPrinter::new(params.threads, params.metric_print_raw, ScalingConfig { ratios: params.scaling_config });
    let (tx_metrics, rx_metrics) = tokio::sync::watch::channel("".to_string());
    let exporter = MetricsExporterBuilder::default()
        .with_aggregation_selector(CustomHistogramSelector::new_with_4k_buckets()?)
        .with_temporality_selector(DefaultTemporalitySelector::new())
        .with_encoder(move |_writer, data| {
            let report = printer.print(&data).map_err(MetricsError::Other)?;
            tx_metrics.send(report).map_err(|e| MetricsError::Other(e.to_string()))?;
            Ok(())
        })
        .build();

    let reader = PeriodicReader::builder(exporter, runtime::Tokio).build();

    let meter_provider = MeterProvider::builder().with_reader(reader).build();
    let meter_provider_copy = meter_provider.clone();
    global::set_meter_provider(meter_provider);

    let meter = global::meter("banking_cohort");
    let meter = Arc::new(meter);

    let h_cohort = tokio::spawn(async move {
        let mut banking_app = BankingApp::new(sdk_config, db_config).await.unwrap();
        let _ = banking_app.init().await;
        let tasks = QueueProcessor::process::<TransferRequest, BankingApp>(rx_queue, meter, params.threads, Arc::new(banking_app)).await;

        let mut i = 1;
        let mut errors_count = 0;
        for task in tasks {
            if let Err(e) = task.await {
                errors_count += 1;
                log::error!("{:?}", e);
            }
            if i % 10 == 0 {
                log::warn!("Initiator thread {} of {} finished.", i, params.threads);
            }

            i += 1;
        }
        log::info!("Finished. Errors count: {}", errors_count);
    });

    let queue_monitor: JoinHandle<Result<(), String>> = start_queue_monitor(rx_queue_ref);

    let all_async_services = tokio::spawn(async move {
        let result = try_join!(h_generator, h_cohort);
        log::warn!("Result from services ={result:?}");
    });

    tokio::select! {
        _ = queue_monitor => {}
        _ = all_async_services => {}

        // CTRL + C termination signal
        _ = signal::ctrl_c() => {
            log::warn!("Shutting down...");
        }
    }

    shutdown_tracer_provider();
    let _ = meter_provider_copy.shutdown();
    let report = rx_metrics.borrow();
    log::warn!("{}", *report);
    Ok(())
}

fn start_queue_monitor(queue: Arc<Receiver<(TransferRequest, f64)>>) -> JoinHandle<Result<(), String>> {
    tokio::spawn(async move {
        let check_frequency = Duration::from_secs(10);
        loop {
            log::warn!("Remaining requests: {}", queue.len());
            tokio::time::sleep(check_frequency).await;
        }
    })
}

async fn get_params() -> Result<LaunchParams, String> {
    let args: Vec<String> = env::args().collect();
    let mut threads: Option<u64> = Some(1);
    let mut accounts: Option<u64> = None;
    let mut target_rate: Option<f32> = None;
    let mut stop_type: Option<StopType> = None;
    let mut scaling_config: Option<HashMap<String, f32>> = None;
    let mut metric_print_raw = None;

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
            } else if param_name.eq("--metric_print_raw") {
                metric_print_raw = Some(true);
            } else if param_name.eq("--metric_scaling") {
                let param_value = &args[i + 1];
                let mut cfg: HashMap<String, f32> = HashMap::new();
                for spec in param_value.replace(' ', "").split(',') {
                    if let Some(i) = spec.find('=') {
                        let metric: String = spec[..i].into();
                        let scale_factor_raw: String = spec[i + 1..].into();
                        let scale_factor = match scale_factor_raw.parse::<f32>() {
                            Err(e) => {
                                log::error!(
                                    "Unable to parse scaling factor for metric '{}'. No scaling will be applied. Pasing: '{}'. Error: {}.",
                                    metric,
                                    scale_factor_raw,
                                    e
                                );
                                1_f32
                            }
                            Ok(scale_factor) => scale_factor,
                        };
                        cfg.insert(metric, scale_factor);
                    }
                }

                if !cfg.is_empty() {
                    scaling_config = Some(cfg);
                }
            }

            i += 2;
        }
    }

    let stop_type = stop_type.ok_or("Parameter --volume is required")?;
    let target_rate = target_rate.ok_or("Parameter --rate is required")?;
    let accounts = accounts.ok_or("Parameter --accounts is required")?;

    Ok(LaunchParams {
        target_rate,
        stop_type,
        threads: threads.unwrap(),
        accounts,
        scaling_config: scaling_config.unwrap_or_default(),
        metric_print_raw: metric_print_raw.is_some(),
    })
}

struct TransferRequestGenerator {
    available_accounts: u64,
    generated: Vec<(u64, u64)>,
}

impl Generator<TransferRequest> for TransferRequestGenerator {
    fn generate(&mut self) -> TransferRequest {
        let mut rnd = rand::thread_rng();
        let mut to;

        let from = rnd.gen_range(1..=self.available_accounts);
        loop {
            to = rnd.gen_range(1..=self.available_accounts);
            if to == from {
                continue;
            }

            let result = self
                .generated
                .iter()
                .find(|(past_from, past_to)| *past_from == from && *past_to == to || *past_from == to && *past_to == from);

            if result.is_none() {
                if self.generated.len() < 100 {
                    self.generated.push((from, to));
                } else {
                    self.generated.remove(0);
                    self.generated.insert(0, (from, to));
                }

                break;
            }
        }

        TransferRequest {
            from: format!("{:<04}", from),
            to: format!("{:<04}", to),
            amount: rust_decimal::Decimal::from_f32(1.0).unwrap(),
        }
    }
}
