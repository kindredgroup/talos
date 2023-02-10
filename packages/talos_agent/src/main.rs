extern crate core;

use log::{debug, info};
use rdkafka::config::RDKafkaLogLevel;
use std::cmp;
use std::sync::Arc;
use talos_agent::api::{AgentConfig, CandidateData, CertificationRequest, CertificationResponse, KafkaConfig, TalosAgentBuilder, TalosAgentType};
use tokio::task::JoinHandle;
use tokio::time;
use tokio::time::Instant;
use uuid::Uuid;

///
/// The sample usage of talos agent library
///

const BATCH_SIZE: i32 = 100;
const IS_ASYNC: bool = true;

fn make_configs() -> (AgentConfig, KafkaConfig) {
    let cohort = "HostForTesting";
    let agent_id = format!("agent-for-{}-{}", cohort, Uuid::new_v4());
    let cfg_agent = AgentConfig {
        agent_id: agent_id.clone(),
        agent_name: format!("agent-for-{}", cohort),
        cohort_name: cohort.to_string(),
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
        created_at: Instant::now(),
    }
}

fn make_agent() -> Box<TalosAgentType> {
    let (cfg_agent, cfg_kafka) = make_configs();

    TalosAgentBuilder::new(cfg_agent)
        .with_kafka(cfg_kafka)
        .build()
        .unwrap_or_else(|e| panic!("{}", format!("Unable to build agent {}", e)))
}

fn get_rate(count: i32, duration_ms: u128) -> u32 {
    ((count as f32) / (duration_ms as f32) * 1000.0) as u32
}

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::init();

    if IS_ASYNC {
        certify_async(BATCH_SIZE).await
    } else {
        certify(BATCH_SIZE).await
    }
}

async fn certify(batch_size: i32) -> Result<(), String> {
    let started_at = time::Instant::now();
    info!("Certifying {} transactions", batch_size);

    let agent = make_agent();
    for _ in 0..batch_size {
        let request = make_candidate(Uuid::new_v4().to_string());
        let rsp = agent.certify(request).await.unwrap();
        debug!("Transaction has been certified. Details: {:?}", rsp);
    }

    let finished_at = time::Instant::now();
    let duration_ms = finished_at.duration_since(started_at).as_millis();
    info!("Finished in {}ms / {}tps", duration_ms, get_rate(batch_size, duration_ms));

    Ok(())
}

async fn certify_async(batch_size: i32) -> Result<(), String> {
    let started_at = time::Instant::now();
    info!("Certifying {} transactions", batch_size);

    let mut tasks = Vec::<JoinHandle<(CertificationResponse, Instant, Instant)>>::new();
    let agent = Arc::new(make_agent());

    for _ in 0..batch_size {
        let ac = Arc::clone(&agent);
        let task = tokio::spawn(async move {
            let request = make_candidate(Uuid::new_v4().to_string());
            let created_at = request.created_at;
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

    metrics.sort();
    let p90i = cmp::min((((metrics.len() * 90) as f32 / 100.0).ceil()) as usize, metrics.len() - 1);
    let p95i = cmp::min((((metrics.len() * 95) as f32 / 100.0).ceil()) as usize, metrics.len() - 1);
    let p75i = cmp::min((((metrics.len() * 75) as f32 / 100.0).ceil()) as usize, metrics.len() - 1);

    let p90 = metrics[p90i];
    let p95 = metrics[p95i];
    let p75 = metrics[p75i];
    let mut p90h: u128 = 0;
    let mut p75h: u128 = 0;
    let mut p95h: u128 = 0;
    let mut p95done = false;
    let mut p90done = false;
    let mut p75done = false;

    for (i, duration) in metrics.iter().enumerate().skip(p75i + 1) {
        if !p75done && *duration != p75 {
            p75done = true;
            p75h = *duration;
        }
        if !p90done && i > p90i && *duration != p90 {
            p90done = true;
            p90h = *duration;
        }
        if !p95done && i > p95i && *duration != p95 {
            p95done = true;
            p95h = *duration;
        }
    }

    if !p75done {
        p75h = metrics[metrics.len() - 1];
    }
    if !p90done {
        p90h = metrics[metrics.len() - 1];
    }
    if !p95done {
        p95h = metrics[metrics.len() - 1];
    }

    let finished_at = time::Instant::now();
    let duration_ms = finished_at.duration_since(started_at).as_millis();
    info!(
        "Finished in {}ms / {}tps. min: {}ms, max: {}ms, p75: {}-{}ms, p90: {}-{}ms, p95: {}-{}ms",
        duration_ms,
        get_rate(batch_size, duration_ms),
        min,
        max,
        p75,
        p75h,
        p90,
        p90h,
        p95,
        p95h
    );

    // info!("{:?}", metrics);

    Ok(())
}
