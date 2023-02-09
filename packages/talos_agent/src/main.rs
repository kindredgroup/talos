use log::{debug, info};
use rdkafka::config::RDKafkaLogLevel;
use std::sync::Arc;
use talos_agent::api::{AgentConfig, CandidateData, CertificationRequest, CertificationResponse, KafkaConfig, TalosAgentBuilder, TalosAgentType};
use tokio::task::JoinHandle;
use tokio::time;
use uuid::Uuid;

///
/// The sample usage of talos agent library
///

const BATCH_SIZE: i32 = 10;
const IS_ASYNC: bool = true;

fn make_configs() -> (AgentConfig, KafkaConfig) {
    let cohort = "HostForTesting";
    let cfg_agent = AgentConfig {
        agent_name: format!("agent-for-{}", cohort),
        cohort_name: cohort.to_string(),
    };

    let cfg_kafka = KafkaConfig {
        brokers: "localhost:9093".to_string(),
        group_id: "agent-test".to_string(),
        enqueue_timeout_ms: 10,
        message_timeout_ms: 5000,
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

    let mut tasks = Vec::<JoinHandle<CertificationResponse>>::new();
    let agent = Arc::new(make_agent());

    for _ in 0..batch_size {
        let ac = Arc::clone(&agent);
        let task = tokio::spawn(async move {
            let request = make_candidate(Uuid::new_v4().to_string());
            ac.certify(request).await.unwrap()
        });
        tasks.push(task);
    }

    for task in tasks {
        let rsp: CertificationResponse = task.await.unwrap();
        debug!("Transaction has been certified. Details: {:?}", rsp);
    }

    let finished_at = time::Instant::now();
    let duration_ms = finished_at.duration_since(started_at).as_millis();
    info!("Finished in {}ms / {}tps", duration_ms, get_rate(batch_size, duration_ms));

    Ok(())
}
