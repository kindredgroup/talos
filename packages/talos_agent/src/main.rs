extern crate core;

use log::{debug, info};
use rdkafka::config::RDKafkaLogLevel;
use std::cmp;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use talos_agent::api::{AgentConfig, CandidateData, CertificationRequest, CertificationResponse, KafkaConfig, TalosAgentBuilder, TalosAgentType};
use tokio::task::JoinHandle;
use tokio::time;
use tokio::time::Instant;
use uuid::Uuid;

///
/// The sample usage of talos agent library
///

const BATCH_SIZE: i32 = 1000;

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
        fetch_wait_max_ms: 1000,
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

fn get_rate(count: i32, duration_ms: u128) -> u32 {
    ((count as f32) / (duration_ms as f32) * 1000.0) as u32
}

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::init();
    certify(BATCH_SIZE).await
}

async fn certify(batch_size: i32) -> Result<(), String> {
    let started_at = time::Instant::now();
    info!("Certifying {} transactions", batch_size);

    let mut tasks = Vec::<JoinHandle<(CertificationResponse, Instant, Instant)>>::new();
    let agent = Arc::new(make_agent().await);

    for _ in 0..batch_size {
        let ac = Arc::clone(&agent);
        let task = tokio::spawn(async move {
            let created_at = Instant::now();
            let request = make_candidate(Uuid::new_v4().to_string());
            let resp = ac.certify(request).await.unwrap();
            (resp, created_at, Instant::now())
        });
        tasks.push(task);
    }

    let mut min: u128 = 50_000;
    let mut max: u128 = 0;
    let mut metrics = Vec::new();
    for task in tasks {
        let (rsp, started_at, finished_at) = task.await.unwrap();
        let d = finished_at.duration_since(started_at).as_millis();
        metrics.push(d);
        if d < min {
            min = d;
        }
        if d > max {
            max = d;
        }
        debug!("Transaction has been certified. Details: {:?}", rsp);
    }

    let finished_at = time::Instant::now();
    let duration_ms = finished_at.duration_since(started_at).as_millis();

    metrics.sort();

    let p50 = Percentile::compute(&metrics, 50, "ms");
    let p75 = Percentile::compute(&metrics, 75, "ms");
    let p90 = Percentile::compute(&metrics, 90, "ms");
    let p95 = Percentile::compute(&metrics, 95, "ms");
    let p99 = Percentile::compute(&metrics, 99, "ms");

    info!(
        "Test duration: {} ms\nThroughput: {} tps\nMin: {} ms\nMax: {} ms\nPercentiles:\n\t{}\n\t{}\n\t{}\n\t{}\n\t{}",
        duration_ms,
        get_rate(batch_size, duration_ms),
        min,
        max,
        p50,
        p75,
        p90,
        p95,
        p99,
    );

    // info!("{:?}", metrics);

    Ok(())
}

struct Percentile {
    percentage: u32,
    value: u128,
    value_high: u128,
    unit: String,
}

impl Percentile {
    fn compute(data_set: &Vec<u128>, percentage: u32, unit: &str) -> Percentile {
        let index = cmp::min((((data_set.len() * percentage as usize) as f32 / 100.0).ceil()) as usize, data_set.len() - 1);
        let value = data_set[index];
        let mut value_high = data_set[data_set.len() - 1];
        for (_i, v) in data_set.iter().enumerate().skip(index + 1) {
            if *v != value {
                value_high = *v;
                break;
            }
        }

        Percentile {
            percentage,
            unit: unit.to_string(),
            value,
            value_high,
        }
    }
}
impl Display for Percentile {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.value_high == self.value {
            write!(f, "p{}: {} {}", self.percentage, self.value, self.unit)
        } else {
            write!(f, "p{}: {} (+{}) {}", self.percentage, self.value, self.value_high - self.value, self.unit)
        }
    }
}
