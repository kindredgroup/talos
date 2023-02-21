extern crate core;

use log::{debug, info};
use rdkafka::config::RDKafkaLogLevel;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use talos_agent::api::{AgentConfig, CandidateData, CertificationRequest, KafkaConfig, TalosAgentBuilder, TalosAgentType, TalosType, TRACK_PUBLISH_METRICS};
use talos_agent::metrics::{format, format_metric, get_rate, PercentileSet, Timing};
use time::OffsetDateTime;
use tokio::task::JoinHandle;
use uuid::Uuid;

///
/// The sample usage of talos agent library
///

const BATCH_SIZE: i32 = 10_000;
const TALOS_TYPE: TalosType = TalosType::InProcessMock;
const PROGRESS_EVERY: i32 = 10_000;
const RATE_100_TPS: Duration = Duration::from_millis(10);
const RATE_200_TPS: Duration = Duration::from_millis(5);
const RATE_500_TPS: Duration = Duration::from_millis(2);
const RATE_1000_TPS: Duration = Duration::from_millis(1);
const RATE_2000_TPS: Duration = Duration::from_nanos(500_000);
const RATE_5000_TPS: Duration = Duration::from_nanos(200_000);
const RATE_10000_TPS: Duration = Duration::from_nanos(100_000);
const RATE_20000_TPS: Duration = Duration::from_nanos(50_000);
const RATE_40000_TPS: Duration = Duration::from_nanos(25_000);
const RATE_80000_TPS: Duration = Duration::from_nanos(12_500);
const RATE_100000_TPS: Duration = Duration::from_nanos(10_000);
const RATE_125000_TPS: Duration = Duration::from_nanos(8_000);
const RATE_160000_TPS: Duration = Duration::from_nanos(6_250);
const RATE_200000_TPS: Duration = Duration::from_nanos(5_000);
const RATE_250000_TPS: Duration = Duration::from_nanos(4_000);
const RATE_320000_TPS: Duration = Duration::from_nanos(3_125);
const RATE_400000_TPS: Duration = Duration::from_nanos(2_500);
const RATE: Duration = RATE_5000_TPS;

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

fn name_rate(v: Duration) -> &'static str {
    match v {
        RATE_100_TPS => "100/s",
        RATE_200_TPS => "200/s",
        RATE_500_TPS => "500/s",
        RATE_1000_TPS => "1000/s",
        RATE_2000_TPS => "2000/s",
        RATE_5000_TPS => "5000/s",
        RATE_10000_TPS => "10000/s",
        RATE_20000_TPS => "20000/s",
        RATE_40000_TPS => "40000/s",
        RATE_80000_TPS => "80000/s",
        RATE_100000_TPS => "100k/s",
        RATE_125000_TPS => "125k/s",
        RATE_160000_TPS => "160k/s",
        RATE_200000_TPS => "200k/s",
        RATE_250000_TPS => "250k/s",
        RATE_320000_TPS => "320k/s",
        RATE_400000_TPS => "400k/s",
        _ => "unknown",
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

async fn make_agentv2(publish_times: &Arc<Mutex<HashMap<String, u64>>>) -> Box<TalosAgentType> {
    let (cfg_agent, cfg_kafka) = make_configs();

    TalosAgentBuilder::new(cfg_agent)
        .with_kafka(cfg_kafka)
        .build_v2(publish_times)
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

    // "global" state where we will collect publishing times: A - B, where
    // B: transaction start time
    // A: the end of call to kafka send_message(candidate)
    let publish_times: Arc<Mutex<HashMap<String, u64>>> = Arc::new(Mutex::new(HashMap::new()));
    let mut tasks = Vec::<JoinHandle<Result<Timing, String>>>::new();
    let agent = Arc::new(make_agentv2(&publish_times).await);

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

    let mut metrics_stage1 = Vec::new(); // incomplete metrics, missing publishing times
    let mut i = 1;
    for task in tasks {
        let r_timing = task.await.unwrap();
        if i % PROGRESS_EVERY == 0 {
            info!("Completed {} of {}", i, batch_size);
        }
        i += 1;
        match r_timing {
            Ok(timing) => {
                metrics_stage1.push(timing.clone());
                debug!("Transaction has been certified. Details: {:?}", timing.response);
            }

            Err(e) => {
                log::error!("{:?}", e);
            }
        };
    }

    // Compute and print metrics

    let finished_at = OffsetDateTime::now_utc().unix_timestamp_nanos();

    let mut total_min = f32::MAX;
    let mut total_max = 0_f32;
    let mut pub_min = f32::MAX;
    let mut pub_max = 0_f32;
    let mut total_min_timing: Option<Timing> = None;
    let mut total_max_timing: Option<Timing> = None;
    let mut pub_min_timing: Option<Timing> = None;
    let mut pub_max_timing: Option<Timing> = None;
    let mut metrics = Vec::new();
    for s1_timing in metrics_stage1 {
        let mut timing = s1_timing.clone();
        let published_at = if TRACK_PUBLISH_METRICS {
            *publish_times.lock().unwrap().get(s1_timing.response.xid.as_str()).unwrap()
        } else {
            0
        };

        timing.publish = Duration::from_nanos(published_at - s1_timing.started_at);
        let d = timing.get_total_ms();
        if d < total_min {
            total_min = d;
            total_min_timing = Some(timing.clone());
        }

        if d > total_max {
            total_max = d;
            total_max_timing = Some(timing.clone());
        }

        if TRACK_PUBLISH_METRICS {
            let p = timing.get_publish_ms();
            if p < pub_min {
                pub_min = p;
                pub_min_timing = Some(timing.clone());
            }
            if p > pub_max {
                pub_max = p;
                pub_max_timing = Some(timing.clone());
            }
        }

        metrics.push(timing.clone());
    }

    let duration_ms = Duration::from_nanos((finished_at - started_at) as u64).as_millis() as u64;

    let total = PercentileSet::new(&mut metrics, Timing::get_total_ms, |i| i.total.as_micros());
    let span1 = PercentileSet::new(&mut metrics, Timing::get_outbox_ms, |i| i.outbox.as_micros());
    let span2 = PercentileSet::new(&mut metrics, Timing::get_receive_and_decide_ms, |i| i.receive_and_decide.as_micros());
    let span3 = PercentileSet::new(&mut metrics, Timing::get_decision_send_ms, |i| i.decision_send.as_micros());
    let span4 = PercentileSet::new(&mut metrics, Timing::get_inbox_ms, |i| i.inbox.as_micros());
    let publish = PercentileSet::new(&mut metrics, Timing::get_publish_ms, |i| i.publish.as_micros());

    let total_min = total_min_timing.unwrap();
    let total_max = total_max_timing.unwrap();

    if TRACK_PUBLISH_METRICS {
        info!(
            "Test duration: {} ms\nThroughput: {} tps\n\n{}\n\n{} \n{} \nPercentiles:\n{}\n{}\n{}\n{}\n{}",
            duration_ms,
            get_rate(batch_size, duration_ms),
            "Metric: value [client->agent.th1 + agent.th1->talos.decision + talos.publish->agent.th1 + agent.th1->client])",
            format_metric("Min".to_string(), total_min.clone()),
            format_metric("Max".to_string(), total_max.clone()),
            format(&total.p50, span1.p50, span2.p50, span3.p50, span4.p50, Some(publish.p50)),
            format(&total.p75, span1.p75, span2.p75, span3.p75, span4.p75, Some(publish.p75.clone())),
            format(&total.p90, span1.p90, span2.p90, span3.p90, span4.p90, Some(publish.p90.clone())),
            format(&total.p95, span1.p95, span2.p95, span3.p95, span4.p95, Some(publish.p95.clone())),
            format(&total.p99, span1.p99, span2.p99, span3.p99, span4.p99, Some(publish.p99)),
        );

        // Print metrics for spreadsheet

        info!(
            "{}\tempty\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            batch_size,
            "Hashmap+actor+task pub".to_string(),
            "FutureProducer".to_string(),
            "In proc mock".to_string(),
            name_rate(RATE),
            get_rate(batch_size, duration_ms),
            total_min.get_total_ms(),
            total_max.get_total_ms(),
            total.p75.value,
            total.p90.value,
            total.p95.value,
            pub_min_timing.unwrap().get_publish_ms(),
            pub_max_timing.unwrap().get_publish_ms(),
            publish.p75.value.clone(),
            publish.p90.value.clone(),
            publish.p95.value.clone(),
        );
    } else {
        info!(
            "Test duration: {} ms\nThroughput: {} tps\n\n{}\n\n{} \n{} \nPercentiles:\n{}\n{}\n{}\n{}\n{}",
            duration_ms,
            get_rate(batch_size, duration_ms),
            "Metric: value [client->agent.th1 + agent.th1->talos.decision + talos.publish->agent.th1 + agent.th1->client])",
            format_metric("Min".to_string(), total_min.clone()),
            format_metric("Max".to_string(), total_max.clone()),
            format(&total.p50, span1.p50, span2.p50, span3.p50, span4.p50, None),
            format(&total.p75, span1.p75, span2.p75, span3.p75, span4.p75, None),
            format(&total.p90, span1.p90, span2.p90, span3.p90, span4.p90, None),
            format(&total.p95, span1.p95, span2.p95, span3.p95, span4.p95, None),
            format(&total.p99, span1.p99, span2.p99, span3.p99, span4.p99, None),
        );

        info!(
            "{}\tempty\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            batch_size,
            "Hashmap+actor+task pub".to_string(),
            "FutureProducer".to_string(),
            "In proc mock".to_string(),
            name_rate(RATE),
            get_rate(batch_size, duration_ms),
            total_min.get_total_ms(),
            total_max.get_total_ms(),
            total.p75.value,
            total.p90.value,
            total.p95.value,
        );
    }

    Ok(())
}
