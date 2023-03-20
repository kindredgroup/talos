extern crate core;
use log::info;
use rdkafka::config::RDKafkaLogLevel;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use talos_agent::agent::errors::AgentError;
use talos_agent::api::{AgentConfig, CandidateData, CertificationRequest, CertificationResponse, KafkaConfig, TalosAgent, TalosType};
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use uuid::Uuid;
use talos_agent::agent::core::TalosAgentImpl;
use talos_agent::agent::model::{CancelRequestChannelMessage, CertifyRequestChannelMessage};
use talos_agent::messaging::api::DecisionMessage;
use talos_agent::messaging::kafka::KafkaInitializer;
use talos_agent::mpsc::core::{ReceiverWrapper, SenderWrapper};

///
/// The sample usage of talos agent library
///

const BATCH_SIZE: i32 = 1000;
const TALOS_TYPE: TalosType = TalosType::InProcessMock;
const PROGRESS_EVERY: i32 = 50_000;
const NANO_IN_SEC: i32 = 1_000_000_000;
const TARGET_RATE: f64 = 500_f64;
const BASE_DELAY: Duration = Duration::from_nanos((NANO_IN_SEC as f64 / TARGET_RATE) as u64);

fn make_configs() -> (AgentConfig, KafkaConfig) {
    let cohort = "HostForTesting";
    let agent = format!("agent-for-{}-{}", cohort, Uuid::new_v4());
    let cfg_agent = AgentConfig {
        agent: agent.clone(),
        cohort: cohort.to_string(),
        buffer_size: 10_000,
        timout_ms: 3_000,
    };

    let cfg_kafka = KafkaConfig {
        brokers: "127.0.0.1:9092".to_string(),
        group_id: agent,
        enqueue_timeout_ms: 10,
        message_timeout_ms: 15000,
        fetch_wait_max_ms: 6000,
        certification_topic: "dev.ksp.certification".to_string(),
        log_level: RDKafkaLogLevel::Info,
        talos_type: TALOS_TYPE,
        sasl_mechanisms: None,
        username: None,
        password: None,
    };

    (cfg_agent, cfg_kafka)
}

fn make_candidate(xid: String) -> CertificationRequest {
    let tx_data = CandidateData {
        xid,
        readset: Vec::new(),
        readvers: Vec::new(),
        snapshot: 5,
        writeset: Vec::from(["3".to_string()]),
    };

    CertificationRequest {
        message_key: "12345".to_string(),
        candidate: tx_data,
        timeout: None, // this will use the default global value as defined in AgentConfig
    }
}

pub fn name_talos_type(talos_type: &TalosType) -> &'static str {
    match talos_type {
        TalosType::External => "Talos",
        TalosType::InProcessMock => "In proc mock",
    }
}

fn name_rate(v: Duration) -> String {
    let nr = 1_f64 / v.as_secs_f64();
    if nr > 1000_f64 {
        format!("{}k/s", nr / 1000_f64)
    } else {
        format!("{}/s", nr)
    }
}

async fn make_agent() -> impl TalosAgent
{
    let (cfg_agent, cfg_kafka) = make_configs();

    // TalosAgentBuilder::new(cfg_agent)
    //     .with_kafka(cfg_kafka)
    //     .with_metrics()
    //     .build()
    //     .await
    //     .unwrap_or_else(|e| panic!("{}", format!("Unable to build agent.\nReason: {}", e)))

    let (tx_certify_ch, rx_certify_ch) = mpsc::channel::<CertifyRequestChannelMessage>(cfg_agent.buffer_size);
    let tx_certify = SenderWrapper::<CertifyRequestChannelMessage> { tx: tx_certify_ch };
    let rx_certify = ReceiverWrapper::<CertifyRequestChannelMessage> { rx: rx_certify_ch };

    let (tx_decision_ch, rx_decision_ch) = mpsc::channel::<DecisionMessage>(cfg_agent.buffer_size);
    let tx_decision = SenderWrapper::<DecisionMessage> { tx: tx_decision_ch };
    let rx_decision = ReceiverWrapper::<DecisionMessage> { rx: rx_decision_ch };

    let (tx_cancel_ch, rx_cancel_ch) = mpsc::channel::<CancelRequestChannelMessage>(cfg_agent.buffer_size);
    let tx_cancel = SenderWrapper::<CancelRequestChannelMessage> { tx: tx_cancel_ch };
    let rx_cancel = ReceiverWrapper::<CancelRequestChannelMessage> { rx: rx_cancel_ch };

    let (publisher, consumer) = KafkaInitializer::connect(cfg_agent.agent.clone(), cfg_kafka).await.expect("Cannot connect to kafka...");

    let agent = TalosAgentImpl::new(
        cfg_agent.clone(),
        Arc::new(Box::new(tx_certify)),
        tx_cancel,
        None,
        Arc::new(None)
    );

    agent.start(rx_certify, rx_cancel, tx_decision, rx_decision, publisher, consumer).expect("unable to start agent");

    agent
}

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::builder().format_timestamp_millis().init();

    certify(BATCH_SIZE).await
}

async fn certify(batch_size: i32) -> Result<(), String> {
    info!("Certifying {} transactions", batch_size);

    // "global" state where we will collect publishing times: A - B, where
    // B: transaction start time
    // A: the end of call to kafka send_message(candidate)
    let mut tasks = Vec::<JoinHandle<Result<CertificationResponse, AgentError>>>::new();
    let agent = Arc::new(make_agent().await);

    let started_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
    info!("Starting publishing process...");

    let adjust_every = 100;
    let mut delay = BASE_DELAY;
    let mut done = 0;
    let mut skip = 0;
    let min_sleep = Duration::from_micros(500);
    for i in 0..batch_size {
        done += 1;
        if skip == 0 {
            thread::sleep(delay);
        } else {
            skip -= 1
        }
        let ac = Arc::clone(&agent);
        let task = tokio::spawn(async move { ac.certify(make_candidate(Uuid::new_v4().to_string())).await });
        tasks.push(task);

        let p = i + 1;
        if p % PROGRESS_EVERY == 0 || p == batch_size {
            info!("Published {} of {}", p, batch_size);
        }

        if done % adjust_every == 0 {
            let now = OffsetDateTime::now_utc().unix_timestamp_nanos();
            let elapsed = Duration::from_nanos((now - started_at) as u64).as_secs_f64();
            let effective_rate = done as f64 / elapsed;
            let scale = TARGET_RATE / effective_rate;
            let sleep = delay.as_secs_f64() / scale;
            if Duration::from_secs_f64(sleep) < min_sleep {
                skip = (min_sleep.as_secs_f64() / sleep).ceil() as i32 * adjust_every;
            } else {
                delay = Duration::from_secs_f64(sleep);
                skip = 0;
            }
        }
    }

    // Collect all stats and produce metrics
    let mut i = 1;
    let mut errors_count = 0;
    for task in tasks {
        if let Err(e) = task.await.unwrap() {
            errors_count += 1;
            log::error!("{:?}", e);
        }

        if i % PROGRESS_EVERY == 0 {
            info!("Completed {} of {}", i, batch_size);
        }
        i += 1;
    }

    // Compute and print metrics

    let finished_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
    let duration_ms = Duration::from_nanos((finished_at - started_at) as u64).as_millis() as u64;

    if let Some(report) = agent.collect_metrics() {
        info!("Metric: value [client->agent + agent->talos.decision + talos.decision->agent + agent->client]) (request publish to kafka)");
        info!("");
        info!("Publishing: {:>5.2} tps", report.publish_rate);
        info!("Throuhput:  {:>5.2} tps", report.get_rate(duration_ms));
        info!("Max: {} ms", report.format_certify_max());
        info!("Min: {} ms", report.format_certify_min());
        info!("99%: {} ms", report.format_p99());
        info!("95%: {} ms", report.format_p95());
        info!("90%: {} ms", report.format_p90());
        info!("75%: {} ms", report.format_p75());
        info!("50%: {} ms", report.format_p50());
        info!("");
        info!("Batch size,Talos type,Target load,Throughput,Min,Max,p75,p90,p95,Errors");
        info!(
            "{},{},{},{},{},{},{},{},{},{}",
            batch_size,
            name_talos_type(&TALOS_TYPE),
            name_rate(BASE_DELAY),
            report.get_rate(duration_ms),
            report.certify_min.get_total_ms(),
            report.certify_max.get_total_ms(),
            report.total.p75.value,
            report.total.p90.value,
            report.total.p95.value,
            errors_count,
        )
    } else {
        log::warn!("There are no metrics collected ...")
    }

    Ok(())
}
