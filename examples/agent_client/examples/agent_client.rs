use async_channel::Receiver;
use examples_support::load_generator::generator::ControlledRateLoadGenerator;
use examples_support::load_generator::models::{Generator, StopType};
use std::collections::HashMap;
use std::env::{var, VarError};
use std::{env, sync::Arc, time::Duration};
use talos_agent::agent::core::TalosAgentImpl;
use talos_agent::agent::model::{CancelRequestChannelMessage, CertifyRequestChannelMessage};
use talos_agent::api::{AgentConfig, CandidateData, CertificationRequest, CertificationResponse, TalosAgent, TalosType};
use talos_agent::messaging::api::DecisionMessage;
use talos_agent::messaging::kafka::KafkaInitializer;
use talos_agent::metrics::client::MetricsClient;
use talos_agent::metrics::core::Metrics;
use talos_agent::metrics::model::Signal;
use talos_agent::mpsc::core::{ReceiverWrapper, SenderWrapper};
use talos_rdkafka_utils::kafka_config::KafkaConfig;
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::{signal, try_join};
use uuid::Uuid;

#[derive(Clone)]
struct LaunchParams {
    stop_type: StopType,
    target_rate: u64,
    threads: u64,
    collect_metrics: bool,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::builder().format_timestamp_millis().init();

    log::info!("started program: {}", std::process::id());

    certify().await
}

async fn certify() -> Result<(), String> {
    let params = get_params().await?;

    let generated = async_channel::unbounded::<(CertificationRequest, f64)>();
    let tx_generated = Arc::new(generated.0);

    // give this to worker threads
    let rx_generated = Arc::new(generated.1);
    // give this to stop controller
    let rx_generated_ref = Arc::clone(&rx_generated);

    let h_monitor: JoinHandle<Result<(), String>> = create_queue_monitor(rx_generated_ref);

    let h_agent_workers = init_workers(params.clone(), rx_generated);

    let h_workload_generator = tokio::spawn(async move {
        let params = params.clone();
        ControlledRateLoadGenerator::generate(params.stop_type, params.target_rate as f32, RequestGenerator {}, tx_generated).await
    });

    let all_async_services = tokio::spawn(async move {
        let result = try_join!(h_workload_generator, h_agent_workers, h_monitor);
        log::info!("Result from the services ={result:?}");
    });

    tokio::select! {
        _ = all_async_services => {}

        // CTRL + C termination signal
        _ = signal::ctrl_c() => {
            log::info!("Shutting down...");
        }
    }

    Ok(())
}

fn init_workers(params: LaunchParams, queue: Arc<Receiver<(CertificationRequest, f64)>>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let agent = Arc::new(make_agent(params.clone()).await);

        let mut tasks: Vec<JoinHandle<Result<u64, String>>> = Vec::new();
        let started_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
        for _ in 1..=params.threads {
            let agent_ref = Arc::clone(&agent);
            let queue_ref = Arc::clone(&queue);
            // implement task
            let task_h = tokio::spawn(async move {
                let mut errors_count = 0_u64;
                loop {
                    let queue = Arc::clone(&queue_ref);
                    let agent = Arc::clone(&agent_ref);
                    if let Ok((tx_req, _)) = queue.recv().await {
                        if (agent.certify(tx_req).await).is_err() {
                            errors_count += 1
                        }
                    } else {
                        break;
                    }
                }

                Ok(errors_count)
            });

            tasks.push(task_h);
        }

        let mut total_errors = 0_u64;
        for task in tasks {
            match task.await {
                Ok(Ok(count)) => total_errors += count,
                Ok(Err(e)) => log::warn!("Agent worker task finished with error: {}", e),
                Err(e) => {
                    log::warn!("Could not launch agent worker task: {}", e);
                }
            }
        }

        if let Some(report) = agent.collect_metrics().await {
            let finished_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
            let duration_ms = Duration::from_nanos((finished_at - started_at) as u64).as_millis() as u64;
            report.print(duration_ms, total_errors);
        } else {
            log::warn!("There are no metrics collected ...")
        }
    })
}

fn load_configs() -> Result<(AgentConfig, KafkaConfig), String> {
    let cfg_agent = AgentConfig {
        agent: read_var("AGENT_NAME").unwrap(),
        cohort: read_var("COHORT_NAME").unwrap(),
        buffer_size: read_var("AGENT_BUFFER_SIZE").unwrap().parse().unwrap(),
        timeout_ms: read_var("AGENT_TIMEOUT_MS").unwrap().parse().unwrap(),
    };

    let mut cfg_kafka = KafkaConfig::from_env(Some("AGENT"));
    let more_producer_values = [
        ("message.timeout.ms".to_string(), "15000".to_string()),
        ("queue.buffering.max.messages".to_string(), "1000000".to_string()),
        ("topic.metadata.refresh.interval.ms".to_string(), "5".to_string()),
        ("socket.keepalive.enable".to_string(), "true".to_string()),
        ("acks".to_string(), "0".to_string()),
    ];

    let more_consumer_values = [
        ("enable.auto.commit".to_string(), "false".to_string()),
        ("auto.offset.reset".to_string(), "latest".to_string()),
        ("fetch.wait.max.ms".to_string(), "600".to_string()),
        ("socket.keepalive.enable".to_string(), "true".to_string()),
        ("acks".to_string(), "0".to_string()),
    ];

    cfg_kafka.extend(Some(HashMap::from(more_producer_values)), Some(HashMap::from(more_consumer_values)));

    Ok((cfg_agent, cfg_kafka))
}

fn read_var(name: &str) -> Result<String, String> {
    match var(name) {
        Ok(value) => {
            if value.is_empty() {
                Err(format!("Environment variable is not set: \"{}\"", name))
            } else {
                Ok(value.trim().to_string())
            }
        }
        Err(e) => match e {
            VarError::NotPresent => Err(format!("Environment variable is not found: \"{}\"", name)),
            VarError::NotUnicode(_) => Err(format!("Environment variable is not unique: \"{}\"", name)),
        },
    }
}

async fn make_agent(params: LaunchParams) -> impl TalosAgent {
    let (cfg_agent, cfg_kafka) = load_configs().unwrap();

    let (tx_certify_ch, rx_certify_ch) = mpsc::channel::<CertifyRequestChannelMessage>(cfg_agent.buffer_size as usize);
    let tx_certify = SenderWrapper::<CertifyRequestChannelMessage> { tx: tx_certify_ch };
    let rx_certify = ReceiverWrapper::<CertifyRequestChannelMessage> { rx: rx_certify_ch };

    let (tx_decision_ch, rx_decision_ch) = mpsc::channel::<DecisionMessage>(cfg_agent.buffer_size as usize);
    let tx_decision = SenderWrapper::<DecisionMessage> { tx: tx_decision_ch };
    let rx_decision = ReceiverWrapper::<DecisionMessage> { rx: rx_decision_ch };

    let (tx_cancel_ch, rx_cancel_ch) = mpsc::channel::<CancelRequestChannelMessage>(cfg_agent.buffer_size as usize);
    let tx_cancel = SenderWrapper::<CancelRequestChannelMessage> { tx: tx_cancel_ch };
    let rx_cancel = ReceiverWrapper::<CancelRequestChannelMessage> { rx: rx_cancel_ch };

    let (publisher, consumer) = KafkaInitializer::connect(cfg_agent.agent.clone(), cfg_kafka, TalosType::External)
        .await
        .expect("Cannot connect to kafka...");

    let metrics: Option<Metrics>;
    let metrics_client: Option<Box<MetricsClient<SenderWrapper<Signal>>>>;
    if params.collect_metrics {
        let server = Metrics::new();

        let (tx, rx) = mpsc::channel::<Signal>(1_000_000_000);
        server.run(ReceiverWrapper { rx });

        let client = MetricsClient {
            tx_destination: SenderWrapper::<Signal> { tx },
        };

        metrics_client = Some(Box::new(client));
        metrics = Some(server);
    } else {
        metrics = None;
        metrics_client = None;
    }

    let agent = TalosAgentImpl::new(
        cfg_agent.clone(),
        Arc::new(Box::new(tx_certify)),
        tx_cancel,
        metrics,
        Arc::new(metrics_client),
        || {
            let (tx, rx) = mpsc::channel::<CertificationResponse>(1);
            (SenderWrapper { tx }, ReceiverWrapper { rx })
        },
    );

    let _ = agent.start(rx_certify, rx_cancel, tx_decision, rx_decision, publisher, consumer);
    agent
}

fn create_queue_monitor(queue: Arc<Receiver<(CertificationRequest, f64)>>) -> JoinHandle<Result<(), String>> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            if queue.is_empty() {
                continue;
            }

            let len = queue.len();
            log::info!("Items remaining to process: {}", len);
        }
    })
}

async fn get_params() -> Result<LaunchParams, String> {
    let args: Vec<String> = env::args().collect();
    let mut threads: Option<u64> = Some(1);
    let mut target_rate: Option<u64> = None;
    let mut stop_type: Option<StopType> = None;
    let mut collect_metrics: Option<bool> = Some(true);

    if args.len() >= 3 {
        let mut i = 1;
        while i < args.len() {
            let param_name = &args[i];
            if param_name.eq("--threads") {
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
            } else if param_name.eq("--no-metrics") {
                collect_metrics = Some(false)
            }
            i += 2;
        }
    }

    if stop_type.is_none() {
        Err("Parameter --volume is required".into())
    } else if target_rate.is_none() {
        Err("Parameter --rate is required".into())
    } else {
        Ok(LaunchParams {
            target_rate: target_rate.unwrap(),
            stop_type: stop_type.unwrap(),
            threads: threads.unwrap(),
            collect_metrics: collect_metrics.unwrap(),
        })
    }
}

struct RequestGenerator {}

impl Generator<CertificationRequest> for RequestGenerator {
    fn generate(&mut self) -> CertificationRequest {
        let tx_data = CandidateData {
            xid: Uuid::new_v4().to_string(),
            readset: Vec::new(),
            readvers: Vec::new(),
            snapshot: 5,
            writeset: Vec::from(["3".to_string()]),
            statemap: None,
        };

        CertificationRequest {
            message_key: "12345".to_string(),
            candidate: tx_data,
            timeout: None, // this will use the default global value as defined in AgentConfig
        }
    }
}
