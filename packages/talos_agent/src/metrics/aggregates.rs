use std::cmp;
use std::fmt::{Display, Formatter};
use std::time::Duration;

const MICRO_PER_MS: f32 = 1_000_f32;

#[derive(Clone, Debug)]
pub struct Timeline {
    pub id: String,
    pub started_at: u64,
    pub outbox: Duration,                              // time candidate spent in the internal channel queue before being sent to kafka
    pub publish: Duration,                             // time between start and publish to kafka
    pub candidate_publish_and_decision_time: Duration, // time from start till talos made a decision (including kafka transmit and read)
    pub decision_download: Duration,
    pub inbox: Duration,
    pub total: Duration,
}

fn ms(value: Duration) -> f32 {
    value.as_micros() as f32 / MICRO_PER_MS
}

impl Timeline {
    pub fn get_total_ms(&self) -> f32 {
        ms(self.total)
    }
    pub fn get_outbox_ms(&self) -> f32 {
        ms(self.outbox)
    }
    pub fn get_candidate_publish_and_decision_time_ms(&self) -> f32 {
        ms(self.candidate_publish_and_decision_time)
    }
    pub fn get_decision_download_ms(&self) -> f32 {
        ms(self.decision_download)
    }
    pub fn get_inbox_ms(&self) -> f32 {
        ms(self.inbox)
    }
    pub fn get_publish_ms(&self) -> f32 {
        ms(self.publish)
    }
}

#[derive(Clone, Debug)]
pub struct Percentile {
    percentage: u32,
    pub value: f32,
    unit: String,
}

impl Percentile {
    fn compute(data_set: &Vec<Timeline>, percentage: u32, unit: &str, get: &impl Fn(&Timeline) -> f32) -> Percentile {
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
pub struct PercentileSet {
    pub p50: Percentile,
    pub p75: Percentile,
    pub p90: Percentile,
    pub p95: Percentile,
    pub p99: Percentile,
}

impl PercentileSet {
    pub fn new<K>(
        metrics: &mut [Timeline],                  // the whole data set
        fn_getter: impl Fn(&Timeline) -> f32,      // function returning value of attribute from Timing object
        fn_key_getter: impl FnMut(&Timeline) -> K, // data set need to be sorted before producing percentiles. This function represent sorting key.
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
