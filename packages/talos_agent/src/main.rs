use rdkafka::config::RDKafkaLogLevel;
use std::sync::Arc;
use talos_agent::api::{AgentConfig, CandidateData, CertificationRequest, CertificationResponse, KafkaConfig, TalosAgentBuilder, TalosAgentType};
use tokio::task::JoinHandle;
use uuid::Uuid;

///
/// The sample usage of talos agent library
///

fn make_configs() -> (AgentConfig, KafkaConfig) {
    let cohort = "HostForTesting";
    let cfg_agent = AgentConfig {
        agent_name: format!("agent-for-{}", cohort),
        cohort_name: cohort.to_string(),
    };

    let cfg_kafka = KafkaConfig {
        brokers: "localhost:9093".to_string(),
        enqueue_timeout_ms: 10,
        message_timeout_ms: 5000,
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
        .build_sc()
        .unwrap_or_else(|e| panic!("{}", format!("Unable to build agent {}", e)))
}

const BATCH_SIZE: i32 = 10;
const IS_ASYNC: bool = true;

#[tokio::main]
async fn main() -> Result<(), String> {
    if IS_ASYNC {
        certify_async(BATCH_SIZE + 1).await
    } else {
        certify(BATCH_SIZE + 1).await
    }
}

async fn certify(batch_size: i32) -> Result<(), String> {
    let agent = make_agent();
    for _ in 1..batch_size {
        let request = make_candidate(Uuid::new_v4().to_string());
        let rsp = agent.certify(request).await.unwrap();
        println!("Transaction has been certified. Details: {:?}", rsp);
    }

    Ok(())
}

async fn certify_async(batch_size: i32) -> Result<(), String> {
    let mut tasks = Vec::<JoinHandle<CertificationResponse>>::new();
    let agent = Arc::new(make_agent());

    for _ in 1..batch_size {
        let ac = Arc::clone(&agent);
        let task = tokio::spawn(async move {
            let request = make_candidate(Uuid::new_v4().to_string());
            ac.certify(request).await.unwrap()
        });
        tasks.push(task);
    }

    for task in tasks {
        let rsp: CertificationResponse = task.await.unwrap();
        println!("Transaction has been certified. Details: {:?}", rsp);
    }

    Ok(())
}
