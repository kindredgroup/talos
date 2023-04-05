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
            let index = cmp::min(
                (((data_set.len() * percentage as usize) as f32 / 100.0).ceil()) as usize - 1,
                data_set.len() - 1,
            );
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
    // $coverage:ignore-start
    // Ignored from coverage because of infinite loop, which just delegates calls to handlers. Handlers are covered separately.
    pub p50: Percentile,
    // $coverage:ignore-end
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

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use super::*;

    fn tx(id: String, total_sec: u64) -> Timeline {
        Timeline {
            id,
            started_at: 0,
            outbox: Duration::from_secs(1),
            publish: Duration::from_secs(2),
            candidate_publish_and_decision_time: Duration::from_secs(3),
            decision_download: Duration::from_secs(4),
            inbox: Duration::from_secs(5),
            total: Duration::from_secs(total_sec),
        }
    }

    #[test]
    fn clone_debug() {
        let p = Percentile {
            percentage: 50,
            value: 1.9,
            unit: "ms".to_string(),
        };

        let ps = PercentileSet {
            p50: p.clone(),
            p75: p.clone(),
            p90: p.clone(),
            p95: p.clone(),
            p99: p,
        };

        let _ = format!("debug and clone coverage {:?}, p50: {}", ps, ps.p50);
    }

    #[test]
    fn compute() {
        let tx1 = tx("id1".to_string(), 1);
        let tx2 = tx("id2".to_string(), 2);
        let tx3 = tx("id3".to_string(), 3);
        let tx4 = tx("id4".to_string(), 4);
        let tx5 = tx("id5".to_string(), 5);
        let tx6 = tx("id6".to_string(), 6);
        let tx7 = tx("id7".to_string(), 7);
        let tx8 = tx("id8".to_string(), 8);
        let tx9 = tx("id9".to_string(), 9);
        let tx10 = tx("id10".to_string(), 10);

        let data = vec![tx1, tx2, tx3, tx4, tx5, tx6, tx7, tx8, tx9, tx10];
        assert_eq!(Percentile::compute(&data, 50, "ms", &Timeline::get_total_ms).value, 5_000_f32);
    }

    #[test]
    fn empty_data_should_compute_to_zero() {
        assert_eq!(Percentile::compute(&Vec::<Timeline>::new(), 50, "ms", &Timeline::get_total_ms).value, 0_f32);
    }

    #[test]
    fn new_percentile_set() {
        let tx1 = tx("id1".to_string(), 1);
        let tx2 = tx("id2".to_string(), 2);
        let tx3 = tx("id3".to_string(), 3);
        let tx4 = tx("id4".to_string(), 4);
        let tx5 = tx("id5".to_string(), 5);
        let tx6 = tx("id6".to_string(), 6);
        let tx7 = tx("id7".to_string(), 7);
        let tx8 = tx("id8".to_string(), 8);
        let tx9 = tx("id9".to_string(), 9);
        let tx10 = tx("id10".to_string(), 10);

        let p_set = PercentileSet::new(&mut [tx1, tx3, tx2, tx5, tx4, tx6, tx8, tx7, tx10, tx9], Timeline::get_total_ms, |t| {
            t.total.as_millis()
        });

        assert_eq!(p_set.p50.value, 5_000_f32);
        assert_eq!(p_set.p75.value, 8_000_f32);
        assert_eq!(p_set.p90.value, 9_000_f32);
        assert_eq!(p_set.p95.value, 10_000_f32);
        assert_eq!(p_set.p99.value, 10_000_f32);
    }

    #[test]
    fn should_convert_spans_to_ms() {
        let tx = tx("id1".to_string(), 1);

        assert_eq!(tx.get_total_ms(), 1_000_f32);
        assert_eq!(tx.get_outbox_ms(), 1_000_f32);
        assert_eq!(tx.get_publish_ms(), 2_000_f32);
        assert_eq!(tx.get_candidate_publish_and_decision_time_ms(), 3_000_f32);
        assert_eq!(tx.get_decision_download_ms(), 4_000_f32);
        assert_eq!(tx.get_inbox_ms(), 5_000_f32);
        assert_eq!(tx.get_total_ms(), 1_000_f32);
    }
}
// $coverage:ignore-end
