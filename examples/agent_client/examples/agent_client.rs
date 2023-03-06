extern crate core;
use log::{debug, info};
use rdkafka::config::RDKafkaLogLevel;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use talos_agent::agentv2::errors::CertifyError;
use talos_agent::api::{AgentConfig, CandidateData, CertificationRequest, KafkaConfig, TalosAgentBuilder, TalosAgentType, TalosType, TRACK_PUBLISH_LATENCY};
use talos_agent::metrics::{format, format_metric, get_rate, name_talos_type, PercentileSet, Timing};
use time::OffsetDateTime;
use tokio::task::JoinHandle;
use uuid::Uuid;

///
/// The sample usage of talos agent library
///

const BATCH_SIZE: i32 = 3;
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

fn name_rate(v: Duration) -> String {
    let nr = 1_f64 / v.as_secs_f64();
    if nr > 1000_f64 {
        format!("{}k/s", nr / 1000_f64)
    } else {
        format!("{}/s", nr)
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

async fn make_agentv2(publish_times: Arc<Mutex<HashMap<String, u64>>>) -> Box<TalosAgentType> {
    let (cfg_agent, cfg_kafka) = make_configs();

    TalosAgentBuilder::new(cfg_agent)
        .with_kafka(cfg_kafka)
        .build_v2(publish_times)
        .await
        .unwrap_or_else(|e| panic!("{}", format!("Unable to build agent.\nReason: {}", e)))
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
    let mut tasks = Vec::<JoinHandle<Result<Timing, CertifyError>>>::new();
    let agent = Arc::new(make_agentv2(Arc::clone(&publish_times)).await);

    // todo: remove this
    // Allow some time for consumer to properly connect
    // log::info!("sleeping for  15 sec ... ");
    // thread::sleep(Duration::from_secs(15));

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
        let task = tokio::spawn(async move { Timing::capture(|| async { ac.certify(make_candidate(Uuid::new_v4().to_string())).await }).await });
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

    let mut metrics_stage1 = Vec::new(); // incomplete metrics, missing publishing times
    let mut i = 1;
    let mut errors_count = 0;
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
                errors_count += 1;
                log::error!("{:?}", e);
            }
        };
    }

    if metrics_stage1.is_empty() {
        return Err(format!("There is no data for metrics. Encountered errors: {}", errors_count));
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

    // Complete stage 1 metrics by adding info about publishing latency

    let mut metrics = Vec::new();
    for s1_timing in metrics_stage1 {
        let mut timing = s1_timing.clone();
        let published_at = if TRACK_PUBLISH_LATENCY {
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

        if TRACK_PUBLISH_LATENCY {
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

    // Compute all spans

    let duration_ms = Duration::from_nanos((finished_at - started_at) as u64).as_millis() as u64;

    let total = PercentileSet::new(&mut metrics, Timing::get_total_ms, |i| i.total.as_micros());
    let span1 = PercentileSet::new(&mut metrics, Timing::get_outbox_ms, |i| i.outbox.as_micros());
    let span2 = PercentileSet::new(&mut metrics, Timing::get_receive_and_decide_ms, |i| i.receive_and_decide.as_micros());
    let span3 = PercentileSet::new(&mut metrics, Timing::get_decision_send_ms, |i| i.decision_send.as_micros());
    let span4 = PercentileSet::new(&mut metrics, Timing::get_inbox_ms, |i| i.inbox.as_micros());
    let publish = PercentileSet::new(&mut metrics, Timing::get_publish_ms, |i| i.publish.as_micros());

    let total_min = total_min_timing.unwrap();
    let total_max = total_max_timing.unwrap();

    // Compute publishing rate
    let mut mi = u64::MAX;
    let mut ma = 0;
    for t in &metrics {
        if mi > t.started_at {
            mi = t.started_at;
        }
        if ma < t.started_at {
            ma = t.started_at;
        }
    }
    let publish_rate_tps = (metrics.len() as f64 / Duration::from_nanos(ma - mi).as_secs_f64()) as i32;

    if TRACK_PUBLISH_LATENCY {
        info!(
            "Test duration: {} ms\nThroughputs (overall / publishing): {}/{} tps\n\n{}\n\n{} \n{} \nPercentiles:\n{}\n{}\n{}\n{}\n{}\nErrors: {}",
            duration_ms,
            get_rate(batch_size, duration_ms),
            publish_rate_tps,
            "Metric: value [client->agent.th1 + agent.th1->talos.decision + talos.publish->agent.th1 + agent.th1->client])",
            format_metric("Min".to_string(), total_min.clone()),
            format_metric("Max".to_string(), total_max.clone()),
            format(&total.p50, span1.p50, span2.p50, span3.p50, span4.p50, Some(publish.p50)),
            format(&total.p75, span1.p75, span2.p75, span3.p75, span4.p75, Some(publish.p75.clone())),
            format(&total.p90, span1.p90, span2.p90, span3.p90, span4.p90, Some(publish.p90.clone())),
            format(&total.p95, span1.p95, span2.p95, span3.p95, span4.p95, Some(publish.p95.clone())),
            format(&total.p99, span1.p99, span2.p99, span3.p99, span4.p99, Some(publish.p99)),
            errors_count
        );

        // Print metrics for spreadsheet

        info!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            batch_size,
            "Hashmap+actor+task pub".to_string(),
            "FutureProducer".to_string(),
            name_talos_type(&TALOS_TYPE),
            name_rate(BASE_DELAY),
            get_rate(batch_size, duration_ms),
            total_min.get_total_ms(),
            total_max.get_total_ms(),
            total.p75.value,
            total.p90.value,
            total.p95.value,
            pub_min_timing.unwrap().get_publish_ms(),
            pub_max_timing.unwrap().get_publish_ms(),
            errors_count,
            publish.p75.value.clone(),
            publish.p90.value.clone(),
            publish.p95.value.clone(),
        );
    } else {
        info!(
            "Test duration: {} ms\nThroughputs (overall / publishing): {}/{}\n\n{}\n\n{} \n{} \nPercentiles:\n{}\n{}\n{}\n{}\n{}\nErrors: {}",
            duration_ms,
            get_rate(batch_size, duration_ms),
            publish_rate_tps,
            "Metric: value [client->agent.th1 + agent.th1->talos.decision + talos.publish->agent.th1 + agent.th1->client])",
            format_metric("Min".to_string(), total_min.clone()),
            format_metric("Max".to_string(), total_max.clone()),
            format(&total.p50, span1.p50, span2.p50, span3.p50, span4.p50, None),
            format(&total.p75, span1.p75, span2.p75, span3.p75, span4.p75, None),
            format(&total.p90, span1.p90, span2.p90, span3.p90, span4.p90, None),
            format(&total.p95, span1.p95, span2.p95, span3.p95, span4.p95, None),
            format(&total.p99, span1.p99, span2.p99, span3.p99, span4.p99, None),
            errors_count,
        );

        info!(
            "{},{},{},{},{},{},{},{},{},{},{},{}",
            batch_size,
            "Hashmap+actor+task pub".to_string(),
            "FutureProducer".to_string(),
            name_talos_type(&TALOS_TYPE),
            name_rate(BASE_DELAY),
            get_rate(batch_size, duration_ms),
            total_min.get_total_ms(),
            total_max.get_total_ms(),
            total.p75.value,
            total.p90.value,
            total.p95.value,
            errors_count,
        );
    }

    // Code below debugs publishing times for each candidate
    let map = &*publish_times.lock().unwrap();
    let mut published;
    for v in map.values() {
        published = *v as i128;
        debug!(
            "\nstarted : {},\nfinished: {},\npublished: {} / {:?}",
            started_at,
            finished_at,
            published,
            (published - started_at) as f32 / 1_000_000_f32,
        );
    }

    Ok(())
}
