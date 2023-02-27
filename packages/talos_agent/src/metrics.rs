use crate::api::{CertificationResponse, TalosType, TRACK_PUBLISH_LATENCY};
use std::cmp;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::time::Duration;
use time::OffsetDateTime;

/// Data structures-needed for metrics

const MICRO_PER_MS: f32 = 1_000_f32;

/// Returns throughput in items per second
pub fn get_rate(count: i32, duration_ms: u64) -> u64 {
    ((count as f32) / (duration_ms as f32) * 1000.0) as u64
}

pub fn name_talos_type(talos_type: &TalosType) -> &'static str {
    match talos_type {
        TalosType::External => "Talos",
        TalosType::InProcessMock => "In proc mock",
    }
}

/// Formats 4 sequential spans as single string value
pub fn format(v: &Percentile, span1: Percentile, span2: Percentile, span3: Percentile, span4: Percentile, publish: Option<Percentile>) -> String {
    match publish {
        Some(p) => {
            format!("\t{} [{} + {} + {} + {}], {}", v, span1.value, span2.value, span3.value, span4.value, p.value)
        }
        None => {
            format!("\t{} [{} + {} + {} + {}]", v, span1.value, span2.value, span3.value, span4.value)
        }
    }
}

pub fn format_metric(metric: String, time: Timing) -> String {
    if TRACK_PUBLISH_LATENCY {
        format!(
            "{}: {} ms [{} + {} + {} + {}], {}",
            metric,
            time.get_total_ms(),
            time.get_outbox_ms(),
            time.get_receive_and_decide_ms(),
            time.get_decision_send_ms(),
            time.get_inbox_ms(),
            time.get_publish_ms(),
        )
    } else {
        format!(
            "{}: {} ms [{} + {} + {} + {}]",
            metric,
            time.get_total_ms(),
            time.get_outbox_ms(),
            time.get_receive_and_decide_ms(),
            time.get_decision_send_ms(),
            time.get_inbox_ms(),
        )
    }
}

pub struct PercentileSet {
    pub p50: Percentile,
    pub p75: Percentile,
    pub p90: Percentile,
    pub p95: Percentile,
    pub p99: Percentile,
}

impl PercentileSet {
    pub fn new<K>(
        metrics: &mut [Timing],                  // the whole data set
        fn_getter: impl Fn(&Timing) -> f32,      // function returning value of attribute from Timing object
        fn_key_getter: impl FnMut(&Timing) -> K, // data set need to be sorted before producing percentiles. This function represent sorting key.
    ) -> Self
    where
        K: Ord,
    {
        let sorted = &mut metrics.to_owned();
        sorted.sort_by_key(fn_key_getter);
        let p50 = Percentile::compute(sorted, 50, "ms", &fn_getter);
        let p75 = Percentile::compute(sorted, 75, "ms", &fn_getter);
        let p90 = Percentile::compute(sorted, 90, "ms", &fn_getter);
        let p95 = Percentile::compute(sorted, 95, "ms", &fn_getter);
        let p99 = Percentile::compute(sorted, 99, "ms", &fn_getter);

        PercentileSet { p50, p75, p90, p95, p99 }
    }
}

#[derive(Clone, Debug)]
pub struct Percentile {
    percentage: u32,
    pub value: f32,
    unit: String,
}

impl Percentile {
    fn compute(data_set: &Vec<Timing>, percentage: u32, unit: &str, get: &impl Fn(&Timing) -> f32) -> Percentile {
        if data_set.is_empty() {
            Percentile {
                percentage,
                unit: unit.to_string(),
                value: 0_f32,
            }
        } else {
            let index = cmp::min((((data_set.len() * percentage as usize) as f32 / 100.0).ceil()) as usize, data_set.len() - 1);

            Percentile {
                percentage,
                unit: unit.to_string(),
                value: get(&data_set[index]),
            }
        }
    }
}

impl Display for Percentile {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "p{}: {} {}", self.percentage, self.value, self.unit)
    }
}

#[derive(Clone, Debug)]
pub struct Timing {
    pub response: CertificationResponse,
    pub started_at: u64,
    pub outbox: Duration,             // time candidate spent in the internal channel queue before being sent to kafka
    pub publish: Duration,            // time between start and publish to kafka
    pub receive_and_decide: Duration, // time from start till talos made a decision (including kafka transmit and read)
    pub decision_send: Duration,
    pub inbox: Duration,
    pub total: Duration,
}

impl Timing {
    pub async fn capture<T>(action: impl Fn() -> T + Send) -> Result<Timing, String>
    where
        T: Future<Output = Result<CertificationResponse, String>>,
    {
        let created_at = OffsetDateTime::now_utc().unix_timestamp_nanos() as u64;

        match action().await {
            Ok(response) => {
                let finished_at = OffsetDateTime::now_utc().unix_timestamp_nanos() as u64;

                let send_started_at = response.send_started_at;
                let decided_at = response.decided_at;
                let decision_buffered_at = response.decision_buffered_at;
                let received_at = response.received_at;

                Ok(Timing {
                    response,
                    started_at: created_at,
                    outbox: Duration::from_nanos(send_started_at - created_at),
                    receive_and_decide: Duration::from_nanos(decided_at - send_started_at),
                    decision_send: Duration::from_nanos(decision_buffered_at - decided_at),
                    inbox: Duration::from_nanos(received_at - decision_buffered_at),
                    total: Duration::from_nanos(finished_at - created_at),
                    publish: Duration::from_nanos(0),
                })
            }

            Err(error) => Err(error),
        }
    }

    pub fn get_total_ms(&self) -> f32 {
        Timing::ms(self.total)
    }
    pub fn get_outbox_ms(&self) -> f32 {
        Timing::ms(self.outbox)
    }
    pub fn get_receive_and_decide_ms(&self) -> f32 {
        Timing::ms(self.receive_and_decide)
    }
    pub fn get_decision_send_ms(&self) -> f32 {
        Timing::ms(self.decision_send)
    }
    pub fn get_inbox_ms(&self) -> f32 {
        Timing::ms(self.inbox)
    }
    pub fn get_publish_ms(&self) -> f32 {
        Timing::ms(self.publish)
    }

    fn ms(value: Duration) -> f32 {
        value.as_micros() as f32 / MICRO_PER_MS
    }
}
