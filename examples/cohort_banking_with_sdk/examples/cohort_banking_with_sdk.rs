use std::str::FromStr;
use std::{collections::HashMap, env, sync::Arc, time::Duration};

use async_channel::Receiver;
use cohort_banking::{app::BankingApp, examples_support::queue_processor::QueueProcessor, model::requests::TransferRequest};
use cohort_sdk::model::Config;
use examples_support::load_generator::models::Generator;
use examples_support::load_generator::{generator::ControlledRateLoadGenerator, models::StopType};

use opentelemetry_api::KeyValue;
use opentelemetry_sdk::Resource;
use rand::Rng;
use rust_decimal::{prelude::FromPrimitive, Decimal};
use tokio::{signal, task::JoinHandle, try_join};

use opentelemetry::global;
use opentelemetry::global::shutdown_tracer_provider;
use opentelemetry::sdk::metrics::{controllers, processors, selectors};

use opentelemetry_prometheus::{Encoder, ExporterConfig, PrometheusExporter, TextEncoder};

#[derive(Clone)]
struct LaunchParams {
    stop_type: StopType,
    target_rate: f32,
    threads: u64,
    accounts: u64,
}

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
        buffer_size: 100_000,
        timeout_ms: 15_000,

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
        // Kafka configs for Replicator
        //
        replicator_client_id: "cohort-banking".into(),
        replicator_group_id: "cohort-banking-replicator".into(),
        producer_config_overrides: HashMap::new(),
        consumer_config_overrides: HashMap::new(),

        //
        // Suffix config values
        //
        /// Initial capacity of the suffix
        suffix_size_max: 500_000,
        /// - The suffix prune threshold from when we start checking if the suffix
        /// should prune.
        /// - Set to None if pruning is not required.
        /// - Defaults to None.
        suffix_prune_at_size: Some(300_000),
        /// Minimum size of suffix after prune.
        /// - Defaults to None.
        suffix_size_min: Some(100_000),

        //
        // Replicator config values
        //
        replicator_buffer_size: 100_000,

        //
        // Database config
        //
        db_pool_size: 200,
        db_user: "postgres".into(),
        db_password: "admin".into(),
        db_host: "127.0.0.1".into(),
        db_port: "5432".into(),
        db_database: "talos-sample-cohort-dev".into(),
    };

    let buckets = [
        0.1, 1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0, 200.0, 300.0, 400.0, 500.0, 1000.0,
        1500.0, 2000.0, 2500.0, 3000.0, 3500.0, 4000.0, 4500.0, 5000.0, 10000.0,
    ];

    let factory = processors::factory(
        selectors::simple::histogram(buckets),
        opentelemetry::sdk::export::metrics::aggregation::cumulative_temporality_selector(),
    );

    let controller = controllers::basic(factory)
        .with_collect_period(Duration::from_secs(20))
        .with_resource(Resource::new([KeyValue::new("service_name", "banking_with_cohort_sdk")]))
        .build();

    // this exporter can export into file
    let exporter = opentelemetry_prometheus::exporter(controller)
        .with_config(ExporterConfig::default().with_scope_info(true))
        .init();

    let meter = global::meter("banking_cohort");
    let meter = Arc::new(meter);

    let h_cohort = tokio::spawn(async move {
        let mut banking_app = BankingApp::new(config).await.unwrap();
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

        log::info!("Finished. errors count: {}", errors_count);
    });

    let h_stop: JoinHandle<Result<(), String>> = start_queue_monitor(rx_queue_ref);

    let all_async_services = tokio::spawn(async move {
        let result = try_join!(h_generator, h_cohort);
        log::warn!("Result from services ={result:?}");
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

    shutdown_tracer_provider();

    print_prometheus_report_as_text(exporter, params.threads);

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
                    "Workers queue is empty and there is no activity signal from replicator. Finishing in: {} seconds...",
                    remaining_attempts * check_frequency.as_secs()
                );
            } else {
                remaining_attempts = total_attempts;
                log::warn!("Counts. Remaining: {}", queue.len(),);
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
        })
    }
}

fn print_prometheus_report_as_text(exporter: PrometheusExporter, threads: u64) {
    let encoder = TextEncoder::new();
    let metric_families = exporter.registry().gather();
    let mut report_buffer = Vec::<u8>::new();
    encoder.encode(&metric_families, &mut report_buffer).unwrap();

    let report: Vec<&str> = match std::str::from_utf8(&report_buffer) {
        Ok(v) => v.split('\n').collect(),
        Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
    };
    // for line in report.iter().filter(|v| v.starts_with("metric_oo_")) {
    //     log::warn!("Printing results = {}", line);
    // }

    print_histogram("Out of Order Install (DB work)", "metric_oo_install_duration", "ms", &report, threads, true);
    print_histogram("Out of Order Install (sleeps)", "metric_oo_wait_duration", "ms", &report, threads, true);
    print_histogram(
        "Out of Order Install (full span)",
        "metric_oo_install_and_wait_duration",
        "ms",
        &report,
        threads,
        true,
    );
    print_histogram("Out of Order Install attempts used", "metric_oo_attempts", "attempts", &report, threads, false);
    print_histogram("Talos roundtrip", "metric_talos", "ms", &report, threads, true);
    print_histogram("Candidate roundtrip", "duration", "ms", &report, threads, true);

    let aborts = extract_num_value::<u64>(&report, "metric_aborts_total");
    let commits = extract_num_value::<u64>(&report, "metric_commits_total");
    let oo_retries = extract_num_value::<u64>(&report, "metric_oo_retry_count_total");
    let oo_giveups = extract_num_value::<u64>(&report, "metric_oo_giveups_count_total");
    let oo_installs = extract_num_value::<u64>(&report, "metric_oo_installs_total");
    let oo_no_data_found = extract_num_value::<u64>(&report, "metric_oo_no_data_found_total");
    let oo_not_safe = extract_num_value::<u64>(&report, "metric_oo_not_safe_count_total");
    let agent_errors = extract_num_value::<u64>(&report, "metric_agent_errors_count_total");
    let db_errors = extract_num_value::<u64>(&report, "metric_db_errors_count_total");

    log::warn!("Commits     : {}", if let Some(v) = commits { v } else { 0 });
    log::warn!("OO installs : {}", if let Some(v) = oo_installs { v } else { 0 });
    log::warn!("OO no data  : {}", if let Some(v) = oo_no_data_found { v } else { 0 });
    log::warn!("OO not safe : {}", if let Some(v) = oo_not_safe { v } else { 0 });
    log::warn!("OO retries  : {}", if let Some(v) = oo_retries { v } else { 0 });
    log::warn!("OO giveups  : {}", if let Some(v) = oo_giveups { v } else { 0 });
    log::warn!("Aborts      : {}", if let Some(v) = aborts { v } else { 0 });
    log::warn!("Agent Errors: {}", if let Some(v) = agent_errors { v } else { 0 });
    log::warn!("DB Errors   : {}", if let Some(v) = db_errors { v } else { 0 });
}

fn print_histogram(name: &str, id: &str, unit: &str, report: &[&str], threads: u64, print_tps: bool) {
    let histogram: Vec<(f64, u64)> = report
        .iter()
        .filter(|v| v.starts_with(format!("{}_bucket", id).as_str()))
        .filter_map(|line| {
            let bucket_label_start_index = line.find("le=\"");
            bucket_label_start_index?;

            let line_remainder = &line[bucket_label_start_index.unwrap() + 4..];
            let bucket_label_end_index = line_remainder.find("\"}");
            bucket_label_end_index?;

            let bucket_label = &line_remainder[..bucket_label_end_index.unwrap()];
            let bucket_label_value = if bucket_label == "+Inf" {
                f64::MAX
            } else {
                bucket_label.parse::<f64>().unwrap()
            };
            let count_in_bucket = &line_remainder[bucket_label_end_index.unwrap() + 3..];
            Some((bucket_label_value, count_in_bucket.parse::<u64>().unwrap()))
        })
        .collect();

    let extracted_count = extract_num_value::<u64>(report, format!("{}_count", id).as_str());
    if let Some(total_count) = extracted_count {
        log::warn!("---------------------------------------------------------");
        log::warn!("{}", name);
        for (bucket, count_in_bucket) in histogram {
            let percents_in_bucket = (100.0 * count_in_bucket as f64) / total_count as f64;
            if bucket == f64::MAX {
                log::warn!("< {:>8} {} : {:>9} : {:>6.2}%", "10000+", unit, count_in_bucket, percents_in_bucket);
            } else {
                log::warn!("< {:>8} {} : {:>9} : {:>6.2}%", bucket, unit, count_in_bucket, percents_in_bucket);
            }
        }

        let rslt_sum = extract_num_value::<f64>(report, format!("{}_sum", id).as_str());
        if let Some(sum) = rslt_sum {
            if unit == "ms" {
                if sum > 1000.0 {
                    log::warn!("Total (sec)           : {:.1}", sum / 1_000_f64);
                    log::warn!("Total (sec) avg per th: {:.1}", sum / 1_000_f64 / threads as f64);
                } else {
                    log::warn!("Total (ms)            : {:.1}", sum);
                    log::warn!("Total (ms) avg per th : {:.1}", sum / threads as f64);
                }
            } else {
                log::warn!("Total ({})            : {:.1}", unit, sum);
                log::warn!("Total ({}) avg per th : {:.1}", unit, sum / threads as f64);
            }

            log::warn!("Count            : {}", total_count);
            if print_tps {
                log::warn!("Throughput (tps) : {:.1}", (total_count as f64) / sum * 1000.0 * threads as f64);
            }
        }
        log::warn!("---------------------------------------------------------\n");
    }
}

fn extract_num_value<T: FromStr + Clone>(report: &[&str], value: &str) -> Option<T> {
    let extracted_as_list: Vec<T> = report
        .iter()
        .filter(|i| i.starts_with(value))
        .filter_map(|i| {
            if let Some(pos) = i.find(' ') {
                let parsed = i[pos + 1..].parse::<T>();
                if let Ok(num) = parsed {
                    Some(num)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    if extracted_as_list.len() == 1 {
        Some(extracted_as_list[0].clone())
    } else {
        None
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
            amount: Decimal::from_f32(1.0).unwrap(),
        }
    }
}
