extern crate core;

use log::{debug, info};
use rdkafka::config::RDKafkaLogLevel;
use std::cmp;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Duration;
use talos_agent::api::{AgentConfig, CandidateData, CertificationRequest, CertificationResponse, KafkaConfig, TalosAgentBuilder, TalosAgentType};
use time::OffsetDateTime;
use tokio::task::JoinHandle;
use uuid::Uuid;

///
/// The sample usage of talos agent library
///

const BATCH_SIZE: i32 = 9000;

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
        brokers: "localhost:9093".to_string(),
        group_id: agent_id,
        enqueue_timeout_ms: 10,
        message_timeout_ms: 15000,
        fetch_wait_max_ms: 6000,
        certification_topic: "dev.ksp.certification".to_string(),
        log_level: RDKafkaLogLevel::Info,
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

async fn make_agent() -> Box<TalosAgentType> {
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
    let started_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
    info!("Certifying {} transactions", batch_size);

    let mut tasks = Vec::<JoinHandle<Timing>>::new();
    let agent = Arc::new(make_agent().await);

    for _ in 0..batch_size {
        let ac = Arc::clone(&agent);
        let task = tokio::spawn(async move {
            let created_at = OffsetDateTime::now_utc().unix_timestamp_nanos() as u64;
            let request = make_candidate(Uuid::new_v4().to_string());
            let resp = ac.certify(request).await.unwrap();
            let decided_at = resp.decided_at;
            let received_at = resp.received_at;
            let send_started_at = resp.send_started_at;
            let decision_buffered_at = resp.decision_buffered_at;
            let finished_at = OffsetDateTime::now_utc().unix_timestamp_nanos() as u64;

            Timing::new(resp, created_at, send_started_at, decided_at, decision_buffered_at, received_at, finished_at)
        });
        tasks.push(task);
    }

    let mut min = 500_000 * 1_000_000;
    let mut max = 0;
    let mut min_timing: Option<Timing> = None;
    let mut max_timing: Option<Timing> = None;
    let mut metrics = Vec::new();
    for task in tasks {
        let timing = task.await.unwrap();
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

    let finished_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
    let duration_ms = Duration::from_nanos((finished_at - started_at) as u64).as_millis() as u64;

    metrics.sort_by_key(|i| i.total.duration.as_millis());
    let p50 = Percentile::compute(&metrics, 50, "ms", Box::new(Timing::get_total));
    let p75 = Percentile::compute(&metrics, 75, "ms", Box::new(Timing::get_total));
    let p90 = Percentile::compute(&metrics, 90, "ms", Box::new(Timing::get_total));
    let p95 = Percentile::compute(&metrics, 95, "ms", Box::new(Timing::get_total));
    let p99 = Percentile::compute(&metrics, 99, "ms", Box::new(Timing::get_total));

    metrics.sort_by_key(|i| i.outbox.duration.as_millis());
    let p50_span1 = Percentile::compute(&metrics, 50, "ms", Box::new(Timing::get_outbox));
    let p75_span1 = Percentile::compute(&metrics, 75, "ms", Box::new(Timing::get_outbox));
    let p90_span1 = Percentile::compute(&metrics, 90, "ms", Box::new(Timing::get_outbox));
    let p95_span1 = Percentile::compute(&metrics, 95, "ms", Box::new(Timing::get_outbox));
    let p99_span1 = Percentile::compute(&metrics, 99, "ms", Box::new(Timing::get_outbox));

    metrics.sort_by_key(|i| i.receive_and_decide.duration.as_millis());
    let p50_span2 = Percentile::compute(&metrics, 50, "ms", Box::new(Timing::get_receive_and_decide));
    let p75_span2 = Percentile::compute(&metrics, 75, "ms", Box::new(Timing::get_receive_and_decide));
    let p90_span2 = Percentile::compute(&metrics, 90, "ms", Box::new(Timing::get_receive_and_decide));
    let p95_span2 = Percentile::compute(&metrics, 95, "ms", Box::new(Timing::get_receive_and_decide));
    let p99_span2 = Percentile::compute(&metrics, 99, "ms", Box::new(Timing::get_receive_and_decide));

    metrics.sort_by_key(|i| i.decision_send.duration.as_millis());
    let p50_span3 = Percentile::compute(&metrics, 50, "ms", Box::new(Timing::get_decision_send));
    let p75_span3 = Percentile::compute(&metrics, 75, "ms", Box::new(Timing::get_decision_send));
    let p90_span3 = Percentile::compute(&metrics, 90, "ms", Box::new(Timing::get_decision_send));
    let p95_span3 = Percentile::compute(&metrics, 95, "ms", Box::new(Timing::get_decision_send));
    let p99_span3 = Percentile::compute(&metrics, 99, "ms", Box::new(Timing::get_decision_send));

    metrics.sort_by_key(|i| i.inbox.duration.as_millis());
    let p50_span4 = Percentile::compute(&metrics, 50, "ms", Box::new(Timing::get_inbox));
    let p75_span4 = Percentile::compute(&metrics, 75, "ms", Box::new(Timing::get_inbox));
    let p90_span4 = Percentile::compute(&metrics, 90, "ms", Box::new(Timing::get_inbox));
    let p95_span4 = Percentile::compute(&metrics, 95, "ms", Box::new(Timing::get_inbox));
    let p99_span4 = Percentile::compute(&metrics, 99, "ms", Box::new(Timing::get_inbox));

    info!(
        "Test duration: {} ms\nThroughput: {} tps\n\n{}\n\n{} \n{} \nPercentiles:\n{}\n{}\n{}\n{}\n{}",
        duration_ms,
        get_rate(batch_size, duration_ms),
        "Metric: value [client->agent.th1 + agent.th1->talos.decision + talos.publish->agent.th1 + agent.th1->client])",
        format_spans("Min".to_string(), min_timing.unwrap()),
        format_spans("Max".to_string(), max_timing.unwrap()),
        format(p50, p50_span1, p50_span2, p50_span3, p50_span4),
        format(p75, p75_span1, p75_span2, p75_span3, p75_span4),
        format(p90, p90_span1, p90_span2, p90_span3, p90_span4),
        format(p95, p95_span1, p95_span2, p95_span3, p95_span4),
        format(p99, p99_span1, p99_span2, p99_span3, p99_span4),
    );

    Ok(())
}

#[derive(Clone, Debug)]
struct Span {
    duration: Duration,
}

impl Span {
    fn new(started_at: u64, ended_at: u64) -> Span {
        Span {
            duration: Duration::from_nanos(ended_at - started_at),
        }
    }
}

struct Percentile {
    percentage: u32,
    value: u64,
    unit: String,
}

impl Percentile {
    fn compute(data_set: &Vec<Timing>, percentage: u32, unit: &str, get: Box<dyn Fn(&Timing) -> u64>) -> Percentile {
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
    outbox: Span,             // time candidate spent in the internal channel queue before being sent to kafka
    receive_and_decide: Span, // time from start till talos made a decision (including kafka transmit and read)
    decision_send: Span,
    inbox: Span,
    total: Span,
}

impl Timing {
    fn new(
        response: CertificationResponse,
        created_at: u64,
        send_started_at: u64,
        decided_at: u64,
        decision_buffered_at: u64,
        received_at: u64,
        finished_at: u64,
    ) -> Self {
        Timing {
            response,
            outbox: Span::new(created_at, send_started_at),
            receive_and_decide: Span::new(send_started_at, decided_at),
            decision_send: Span::new(decided_at, decision_buffered_at),
            inbox: Span::new(decision_buffered_at, received_at),
            total: Span::new(created_at, finished_at),
        }
    }
    fn get_total(&self) -> u64 {
        self.total.duration.as_millis() as u64
    }
    fn get_outbox(&self) -> u64 {
        self.outbox.duration.as_millis() as u64
    }
    fn get_receive_and_decide(&self) -> u64 {
        self.receive_and_decide.duration.as_millis() as u64
    }
    fn get_decision_send(&self) -> u64 {
        self.decision_send.duration.as_millis() as u64
    }
    fn get_inbox(&self) -> u64 {
        self.inbox.duration.as_millis() as u64
    }
}
