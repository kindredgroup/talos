extern crate core;

use log::{debug, info};
use rdkafka::config::RDKafkaLogLevel;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use talos_agent::api::{AgentConfig, CandidateData, CertificationRequest, KafkaConfig, TalosAgentBuilder, TalosAgentType, TalosType};
use talos_agent::metrics::{format, format_metric, get_rate, PercentileSet, Timing};
use time::OffsetDateTime;
use tokio::task::JoinHandle;
use uuid::Uuid;

///
/// The sample usage of talos agent library
///

const BATCH_SIZE: i32 = 10_000;
const TALOS_TYPE: TalosType = TalosType::External;
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

// async fn make_agent() -> Box<TalosAgentType> {
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

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::init();
    certify(BATCH_SIZE).await
}

async fn certify(batch_size: i32) -> Result<(), String> {
    info!("Certifying {} transactions", batch_size);

    let mut tasks = Vec::<JoinHandle<Timing>>::new();
    let agent = Arc::new(make_agentv2().await);

    // Allow some time for consumer to properly connect
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

    // Collect all stats and produce metrics

    let mut min = f32::MAX;
    let mut max = f32::MIN;
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
        let d = timing.get_total_ms();
        if d < min {
            min = d;
            min_timing = Some(timing.clone());
        }
        if d > max {
            max = d;
            max_timing = Some(timing.clone());
        }

        metrics.push(timing.clone());
        debug!("Transaction has been certified. Details: {:?}", timing.response);
    }

    // Compute and print metrics

    let finished_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
    let duration_ms = Duration::from_nanos((finished_at - started_at) as u64).as_millis() as u64;

    let total = PercentileSet::new(&mut metrics, Timing::get_total_ms, |i| i.total.as_micros());
    let span1 = PercentileSet::new(&mut metrics, Timing::get_outbox_ms, |i| i.outbox.as_micros());
    let span2 = PercentileSet::new(&mut metrics, Timing::get_receive_and_decide_ms, |i| i.receive_and_decide.as_micros());
    let span3 = PercentileSet::new(&mut metrics, Timing::get_decision_send_ms, |i| i.decision_send.as_micros());
    let span4 = PercentileSet::new(&mut metrics, Timing::get_inbox_ms, |i| i.inbox.as_micros());

    info!(
        "Test duration: {} ms\nThroughput: {} tps\n\n{}\n\n{} \n{} \nPercentiles:\n{}\n{}\n{}\n{}\n{}",
        duration_ms,
        get_rate(batch_size, duration_ms),
        "Metric: value [client->agent.th1 + agent.th1->talos.decision + talos.publish->agent.th1 + agent.th1->client])",
        format_metric("Min".to_string(), min_timing.unwrap()),
        format_metric("Max".to_string(), max_timing.unwrap()),
        format(total.p50, span1.p50, span2.p50, span3.p50, span4.p50),
        format(total.p75, span1.p75, span2.p75, span3.p75, span4.p75),
        format(total.p90, span1.p90, span2.p90, span3.p90, span4.p90),
        format(total.p95, span1.p95, span2.p95, span3.p95, span4.p95),
        format(total.p99, span1.p99, span2.p99, span3.p99, span4.p99),
    );

    Ok(())
}
