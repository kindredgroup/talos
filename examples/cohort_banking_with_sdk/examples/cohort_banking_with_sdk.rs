use std::{collections::HashMap, env, sync::Arc, time::Duration};

use async_channel::Receiver;
use cohort_banking::{app::BankingApp, examples_support::queue_processor::QueueProcessor, model::requests::TransferRequest};
use cohort_sdk::model::Config;
use examples_support::load_generator::models::Generator;
use examples_support::load_generator::{generator::ControlledRateLoadGenerator, models::StopType};

use metrics::model::MinMax;
use metrics::opentel::aggregation_selector::CustomHistogramSelector;
use metrics::opentel::printer::MetricsToStringPrinter;
use metrics::opentel::scaling::ScalingConfig;
use opentelemetry_api::global;
use opentelemetry_api::metrics::MetricsError;
use opentelemetry_sdk::metrics::reader::DefaultTemporalitySelector;
use opentelemetry_sdk::metrics::{MeterProvider, PeriodicReader};
use opentelemetry_sdk::runtime;
use opentelemetry_stdout::MetricsExporterBuilder;
use rand::Rng;
use rust_decimal::prelude::FromPrimitive;
use tokio::{signal, task::JoinHandle, try_join};

// use metrics::opentel::global;
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

/// Connects to database, to kafka certification topic as talso agent and as cohort replicator.
/// Generates some number of banking transactions and passes then all to Talos for certification.
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

    let (tx_queue, rx_queue) = async_channel::unbounded::<TransferRequest>();
    let rx_queue = Arc::new(rx_queue);
    let rx_queue_ref = Arc::clone(&rx_queue);

    let generator_impl = TransferRequestGenerator {
        available_accounts: params.accounts,
        generated: Vec::new(),
    };

    let generator = ControlledRateLoadGenerator::generate(params.stop_type, params.target_rate, generator_impl, Arc::new(tx_queue));
    let h_generator = tokio::spawn(generator);

    let config = Config {
        //
        // cohort configs
        //
        retry_attempts_max: 10,
        retry_backoff_max_ms: 1500,
        retry_oo_backoff_max_ms: 1000,
        retry_oo_attempts_max: 10,

        //
        // agent config values
        //
        agent: "cohort-banking".into(),
        cohort: "cohort-banking".into(),
        // The size of internal buffer for candidates
        buffer_size: 10_000_000,
        timeout_ms: 600_000,

        //
        // Common to kafka configs values
        //
        brokers: "127.0.0.1:9092".into(),
        topic: "dev.ksp.certification".into(),
        sasl_mechanisms: None,
        kafka_username: None,
        kafka_password: None,

        //
        // Kafka configs for Agent
        //
        // Must be unique for each agent instance. Can be the same as AgentConfig.agent_id
        agent_group_id: "cohort-banking".into(),
        agent_fetch_wait_max_ms: 6000,
        // The maximum time librdkafka may use to deliver a message (including retries)
        agent_message_timeout_ms: 15000,
        // Controls how long to wait until message is successfully placed on the librdkafka producer queue  (including retries).
        agent_enqueue_timeout_ms: 10,
        // should be mapped to rdkafka::config::RDKafkaLogLevel
        agent_log_level: 6,

        //
        // Database config
        //
        db_pool_size: 100,
        db_user: "admin".into(),
        db_password: "admin".into(),
        db_host: "127.0.0.1".into(),
        db_port: "5432".into(),
        db_database: "talos-sample-cohort-dev".into(),
    };

    let scaling_config = ScalingConfig { ratios: params.scaling_config };
    let printer = MetricsToStringPrinter::new(params.threads, params.metric_print_raw);
    let (tx_metrics, rx_metrics) = tokio::sync::watch::channel("".to_string());
    let exporter = MetricsExporterBuilder::default()
        .with_aggregation_selector(CustomHistogramSelector::new_with_4k_buckets()?)
        .with_temporality_selector(DefaultTemporalitySelector::new())
        .with_encoder(move |_writer, data| {
            let report = printer.print(&data, &scaling_config).map_err(MetricsError::Other)?;
            tx_metrics.send(report).map_err(|e| MetricsError::Other(e.to_string()))?;
            Ok(())
        })
        .build();

    let reader = PeriodicReader::builder(exporter, runtime::Tokio).build();

    let meter_provider = MeterProvider::builder().with_reader(reader).build();
    global::set_meter_provider(meter_provider.clone());

    // let scaling_config = ScalingConfig { ratios: params.scaling_config };
    // global::set_scaling_config(scaling_config);

    let meter = global::meter("banking_cohort");
    let meter = Arc::new(meter);

    let h_cohort = tokio::spawn(async move {
        let mut banking_app = BankingApp::new(config).await.unwrap();
        let _ = banking_app.init().await;
        let tasks = QueueProcessor::process::<TransferRequest, BankingApp>(rx_queue, meter, params.threads, Arc::new(banking_app)).await;

        let mut i = 1;
        let mut errors_count = 0;
        let mut timeline = MinMax::default();
        for task in tasks {
            match task.await {
                Err(e) => {
                    errors_count += 1;
                    log::error!("{:?}", e);
                }
                Ok(thread_timeline) => timeline.merge(thread_timeline),
            }
            if i % 10 == 0 {
                log::warn!("Initiator thread {} of {} finished.", i, params.threads);
            }

            i += 1;
        }

        log::warn!("Duration: {}", Duration::from_nanos((timeline.max - timeline.min) as u64).as_secs_f32());
        log::info!("Finished. errors count: {}", errors_count);
    });

    let h_stop: JoinHandle<Result<(), String>> = start_queue_monitor(rx_queue_ref);

    let all_async_services = tokio::spawn(async move {
        let result = try_join!(h_generator, h_cohort, h_stop);
        log::warn!("Result from services ={result:?}");
    });

    tokio::select! {
        _ = all_async_services => {}

        // CTRL + C termination signal
        _ = signal::ctrl_c() => {
            log::warn!("Shutting down...");
        }
    }

    shutdown_tracer_provider();
    let _ = meter_provider.shutdown();

    let report = rx_metrics.borrow();
    log::warn!("{}", *report);
    Ok(())
}

fn start_queue_monitor(queue: Arc<Receiver<TransferRequest>>) -> JoinHandle<Result<(), String>> {
    tokio::spawn(async move {
        let check_frequency = Duration::from_secs(10);
        let total_attempts = 3;

        let mut remaining_attempts = total_attempts;
        loop {
            if remaining_attempts == 0 {
                // we consumed all attempts
                break;
            }

            if queue.is_empty() {
                // queue is empty and there are no signals from other workers, reduce window and try again
                remaining_attempts -= 1;
                log::warn!(
                    "Workers queue is empty. Finishing in: {} seconds...",
                    (remaining_attempts + 1) * check_frequency.as_secs()
                );
            } else {
                remaining_attempts = total_attempts;
                log::warn!("Remaining requests: {}", queue.len());
            }

            tokio::time::sleep(check_frequency).await;
        }

        queue.close();

        Err("Signal from StopController".into())
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
            accounts: accounts.unwrap(),
            scaling_config: scaling_config.unwrap(),
            metric_print_raw: metric_print_raw.is_some(),
        })
    }
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
