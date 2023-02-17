use crate::api::CertificationResponse;
use std::cmp;
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::time::Duration;
use time::OffsetDateTime;

/// Data structures-needed for metrics

/// Returns throughput in items per second
pub fn get_rate(count: i32, duration_ms: u64) -> u64 {
    ((count as f32) / (duration_ms as f32) * 1000.0) as u64
}

/// Formats 4 sequential spans as single string value
pub fn format(v: Percentile, span1: Percentile, span2: Percentile, span3: Percentile, span4: Percentile) -> String {
    format!("\t{} [{} + {} + {} + {}]", v, span1.value, span2.value, span3.value, span4.value)
}

pub fn format_metric(metric: String, time: Timing) -> String {
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

pub struct PercentileSet {
    pub p50: Percentile,
    pub p75: Percentile,
    pub p90: Percentile,
    pub p95: Percentile,
    pub p99: Percentile,
}

impl PercentileSet {
    pub fn new<K>(
        metrics: &Vec<Timing>,                   // the whole data set
        fn_getter: impl Fn(&Timing) -> u64,      // function returning value of attribute from Timing object
        fn_key_getter: impl FnMut(&Timing) -> K, // data set need to be sorted before producing percentiles. This function represent sorting key.
    ) -> Self
    where
        K: Ord,
    {
        let _ = &metrics.clone().sort_by_key(fn_key_getter);
        let p50 = Percentile::compute(metrics, 50, "ms", &fn_getter);
        let p75 = Percentile::compute(metrics, 75, "ms", &fn_getter);
        let p90 = Percentile::compute(metrics, 90, "ms", &fn_getter);
        let p95 = Percentile::compute(metrics, 95, "ms", &fn_getter);
        let p99 = Percentile::compute(metrics, 99, "ms", &fn_getter);

        PercentileSet { p50, p75, p90, p95, p99 }
    }
}

pub struct Percentile {
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
pub struct Timing {
    pub response: CertificationResponse,
    pub outbox: Duration,             // time candidate spent in the internal channel queue before being sent to kafka
    pub receive_and_decide: Duration, // time from start till talos made a decision (including kafka transmit and read)
    pub decision_send: Duration,
    pub inbox: Duration,
    pub total: Duration,
}

impl Timing {
    pub async fn capture<T>(action: impl Fn() -> T + Send) -> Self
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

    pub fn get_total(&self) -> u64 {
        self.total.as_millis() as u64
    }
    pub fn get_outbox(&self) -> u64 {
        self.outbox.as_millis() as u64
    }
    pub fn get_receive_and_decide(&self) -> u64 {
        self.receive_and_decide.as_millis() as u64
    }
    pub fn get_decision_send(&self) -> u64 {
        self.decision_send.as_millis() as u64
    }
    pub fn get_inbox(&self) -> u64 {
        self.inbox.as_millis() as u64
    }
}
