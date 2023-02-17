extern crate core;

use log::{debug, info};
use rdkafka::config::RDKafkaLogLevel;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use std::{cmp, thread};
use talos_agent::api::{AgentConfig, CandidateData, CertificationRequest, CertificationResponse, KafkaConfig, TalosAgentBuilder, TalosAgentType, TalosType};
use time::OffsetDateTime;
use tokio::task::JoinHandle;
use uuid::Uuid;

///
/// The sample usage of talos agent library
///

const BATCH_SIZE: i32 = 1_000;
const TALOS_TYPE: TalosType = TalosType::InProcessMock;
const PROGRESS_EVERY: i32 = 10_000;
// const RATE_100_TPS: Duration = Duration::from_millis(10);
// const RATE_200_TPS: Duration = Duration::from_millis(5);
// const RATE_500_TPS: Duration = Duration::from_millis(2);
// const RATE_1000_TPS: Duration = Duration::from_millis(1);
const RATE_2000_TPS: Duration = Duration::from_nanos(500_000);
// const RATE_5000_TPS: Duration = Duration::from_nanos(200_000);
// const RATE_10000_TPS: Duration = Duration::from_nanos(100_000);
const RATE: Duration = RATE_2000_TPS;

fn make_configs() -> (AgentConfig, KafkaConfig) {
    let cohort = "HostForTesting";
    let agent_id = format!("agent-for-{}-{}", cohort, Uuid::new_v4());
    let cfg_agent = AgentConfig {
        agent_id: agent_id.clone(),
        agent_name: format!("agent-for-{}", cohort),
        cohort_name: cohort.to_string(),
        buffer_size: 10_000,
    };

    let cfg_kafka = KafkaConfig {
        brokers: "127.0.0.1:9093".to_string(),
        group_id: agent_id,
        enqueue_timeout_ms: 10,
        message_timeout_ms: 15000,
        fetch_wait_max_ms: 6000,
        certification_topic: "dev.ksp.certification".to_string(),
        log_level: RDKafkaLogLevel::Info,
        talos_type: TALOS_TYPE,
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
    }
}

// fn make_agent() -> Box<TalosAgentType> {
//     let (cfg_agent, cfg_kafka) = make_configs();
//
//     TalosAgentBuilder::new(cfg_agent)
//         .with_kafka(cfg_kafka)
//         .build()
//         .unwrap_or_else(|e| panic!("{}", format!("Unable to build agent {}", e)))
// }

async fn make_agentv2() -> Box<TalosAgentType> {
    let (cfg_agent, cfg_kafka) = make_configs();

    TalosAgentBuilder::new(cfg_agent)
        .with_kafka(cfg_kafka)
        .build_v2()
        .await
        .unwrap_or_else(|e| panic!("{}", format!("Unable to build agent {}", e)))
}

fn get_rate(count: i32, duration_ms: u64) -> u64 {
    ((count as f32) / (duration_ms as f32) * 1000.0) as u64
}

fn format(v: Percentile, span1: Percentile, span2: Percentile, span3: Percentile, span4: Percentile) -> String {
    format!("\t{} [{} + {} + {} + {}]", v, span1.value, span2.value, span3.value, span4.value)
}

fn format_spans(metric: String, time: Timing) -> String {
    format!(
        "{}: {} ms [{} + {} + {} + {}]",
        metric,
        time.get_total(),
        time.get_outbox(),
        time.get_receive_and_decide(),
        time.get_decision_send(),
        time.get_inbox(),
    )
}

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::init();
    certify(BATCH_SIZE).await
}

async fn certify(batch_size: i32) -> Result<(), String> {
    info!("Certifying {} transactions", batch_size);

    let mut tasks = Vec::<JoinHandle<Timing>>::new();
    let agent = Arc::new(make_agentv2().await);

    thread::sleep(Duration::from_secs(5));
    let started_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
    info!("Starting publishing process...");

    for i in 0..batch_size {
        thread::sleep(RATE);
        let ac = Arc::clone(&agent);
        let task = tokio::spawn(async move { Timing::capture(|| async { ac.certify(make_candidate(Uuid::new_v4().to_string())).await }).await });
        tasks.push(task);

        let p = i + 1;
        if p % PROGRESS_EVERY == 0 || p == batch_size {
            info!("Published {} of {}", p, batch_size);
        }
    }

    let mut min = 500_000 * 1_000_000;
    let mut max = 0;
    let mut min_timing: Option<Timing> = None;
    let mut max_timing: Option<Timing> = None;
    let mut metrics = Vec::new();
    let mut i = 1;
    for task in tasks {
        let timing = task.await.unwrap();
        if i % PROGRESS_EVERY == 0 {
            info!("Completed {} of {}", i, batch_size);
        }
        i += 1;
        let rsp = timing.response.clone();
        let d = timing.get_total();
        if d < min {
            min = d;
            min_timing = Some(timing.clone());
        }
        if d > max {
            max = d;
            max_timing = Some(timing.clone());
        }

        metrics.push(timing.clone());
        debug!("Transaction has been certified. Details: {:?}", rsp);
    }

    // Compute and print metrics

    let finished_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
    let duration_ms = Duration::from_nanos((finished_at - started_at) as u64).as_millis() as u64;

    metrics.sort_by_key(|i| i.total.as_millis());
    let total = PercentileSet::new(&metrics, Timing::get_total);

    metrics.sort_by_key(|i| i.outbox.as_millis());
    let span1 = PercentileSet::new(&metrics, Timing::get_outbox);

    metrics.sort_by_key(|i| i.receive_and_decide.as_millis());
    let span2 = PercentileSet::new(&metrics, Timing::get_receive_and_decide);

    metrics.sort_by_key(|i| i.decision_send.as_millis());
    let span3 = PercentileSet::new(&metrics, Timing::get_decision_send);

    metrics.sort_by_key(|i| i.inbox.as_millis());
    let span4 = PercentileSet::new(&metrics, Timing::get_inbox);

    info!(
        "Test duration: {} ms\nThroughput: {} tps\n\n{}\n\n{} \n{} \nPercentiles:\n{}\n{}\n{}\n{}\n{}",
        duration_ms,
        get_rate(batch_size, duration_ms),
        "Metric: value [client->agent.th1 + agent.th1->talos.decision + talos.publish->agent.th1 + agent.th1->client])",
        format_spans("Min".to_string(), min_timing.unwrap()),
        format_spans("Max".to_string(), max_timing.unwrap()),
        format(total.p50, span1.p50, span2.p50, span3.p50, span4.p50),
        format(total.p75, span1.p75, span2.p75, span3.p75, span4.p75),
        format(total.p90, span1.p90, span2.p90, span3.p90, span4.p90),
        format(total.p95, span1.p95, span2.p95, span3.p95, span4.p95),
        format(total.p99, span1.p99, span2.p99, span3.p99, span4.p99),
    );

    Ok(())
}

struct PercentileSet {
    p50: Percentile,
    p75: Percentile,
    p90: Percentile,
    p95: Percentile,
    p99: Percentile,
}

impl PercentileSet {
    fn new(metrics: &Vec<Timing>, getter: impl Fn(&Timing) -> u64) -> Self {
        let p50 = Percentile::compute(metrics, 50, "ms", &getter);
        let p75 = Percentile::compute(metrics, 75, "ms", &getter);
        let p90 = Percentile::compute(metrics, 90, "ms", &getter);
        let p95 = Percentile::compute(metrics, 95, "ms", &getter);
        let p99 = Percentile::compute(metrics, 99, "ms", &getter);

        PercentileSet { p50, p75, p90, p95, p99 }
    }
}

struct Percentile {
    percentage: u32,
    value: u64,
    unit: String,
}

impl Percentile {
    fn compute(data_set: &Vec<Timing>, percentage: u32, unit: &str, get: &impl Fn(&Timing) -> u64) -> Percentile {
        let index = cmp::min((((data_set.len() * percentage as usize) as f32 / 100.0).ceil()) as usize, data_set.len() - 1);

        Percentile {
            percentage,
            unit: unit.to_string(),
            value: get(&data_set[index]),
        }
    }
}

impl Display for Percentile {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "p{}: {} {}", self.percentage, self.value, self.unit)
    }
}

#[derive(Clone, Debug)]
struct Timing {
    response: CertificationResponse,
    outbox: Duration,             // time candidate spent in the internal channel queue before being sent to kafka
    receive_and_decide: Duration, // time from start till talos made a decision (including kafka transmit and read)
    decision_send: Duration,
    inbox: Duration,
    total: Duration,
}

impl Timing {
    async fn capture<T>(action: impl Fn() -> T + Send) -> Self
    where
        T: Future<Output = Result<CertificationResponse, String>>,
    {
        let created_at = OffsetDateTime::now_utc().unix_timestamp_nanos() as u64;
        let response: CertificationResponse = action().await.unwrap();
        let finished_at = OffsetDateTime::now_utc().unix_timestamp_nanos() as u64;

        let send_started_at = response.send_started_at;
        let decided_at = response.decided_at;
        let decision_buffered_at = response.decision_buffered_at;
        let received_at = response.received_at;

        Timing {
            response,
            outbox: Duration::from_nanos(send_started_at - created_at),
            receive_and_decide: Duration::from_nanos(decided_at - send_started_at),
            decision_send: Duration::from_nanos(decision_buffered_at - decided_at),
            inbox: Duration::from_nanos(received_at - decision_buffered_at),
            total: Duration::from_nanos(finished_at - created_at),
        }
    }

    fn get_total(&self) -> u64 {
        self.total.as_millis() as u64
    }
    fn get_outbox(&self) -> u64 {
        self.outbox.as_millis() as u64
    }
    fn get_receive_and_decide(&self) -> u64 {
        self.receive_and_decide.as_millis() as u64
    }
    fn get_decision_send(&self) -> u64 {
        self.decision_send.as_millis() as u64
    }
    fn get_inbox(&self) -> u64 {
        self.inbox.as_millis() as u64
    }
}
