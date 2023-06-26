use std::cmp;
use std::fmt::{Display, Formatter};
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Timeline {
    pub id: String,
    pub started_at: u64,
    pub outbox_1: Duration,               // time candidate spent in the internal channel queue before received by agent
    pub enqueing_2: Duration,             // time candidate spent inserting to internal waiting list (in flight queue)
    pub publish_taks_spawn_3: Duration,   // time spent spawning publisher task
    pub candidate_publish_4: Duration,    // time spent publishing to kafka
    pub candidate_kafka_trip_5: Duration, // time spent in kafka
    pub decision_duration_6: Duration,    // time from the moment candidate is picked up from kafka till decision is made
    pub decision_download_7: Duration,    // the duration of decision delivery via kafka to agent
    pub inbox_8: Duration,
    pub total: Duration,
}

fn micros(value: Duration) -> f32 {
    value.as_micros() as f32
}

impl Timeline {
    pub fn get_started_at(&self) -> u64 {
        self.started_at
    }
    pub fn get_total_mcs(&self) -> f32 {
        micros(self.total)
    }
    pub fn get_outbox_mcs(&self) -> f32 {
        micros(self.outbox_1)
    }
    pub fn get_enqueing_mcs(&self) -> f32 {
        micros(self.enqueing_2)
    }
    pub fn get_publish_taks_spawn_mcs(&self) -> f32 {
        micros(self.publish_taks_spawn_3)
    }
    pub fn get_candidate_publish_mcs(&self) -> f32 {
        micros(self.candidate_publish_4)
    }
    pub fn get_candidate_kafka_trip_mcs(&self) -> f32 {
        micros(self.candidate_kafka_trip_5)
    }
    pub fn get_decision_duration_mcs(&self) -> f32 {
        micros(self.decision_duration_6)
    }
    pub fn get_decision_download_mcs(&self) -> f32 {
        micros(self.decision_download_7)
    }
    pub fn get_inbox_mcs(&self) -> f32 {
        micros(self.inbox_8)
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
        let p50 = Percentile::compute(sorted, 50, "mcs", &fn_getter);
        let p75 = Percentile::compute(sorted, 75, "mcs", &fn_getter);
        let p90 = Percentile::compute(sorted, 90, "mcs", &fn_getter);
        let p95 = Percentile::compute(sorted, 95, "mcs", &fn_getter);
        let p99 = Percentile::compute(sorted, 99, "mcs", &fn_getter);

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
            outbox_1: Duration::from_secs(1),
            enqueing_2: Duration::from_secs(2),
            publish_taks_spawn_3: Duration::from_secs(3),
            candidate_publish_4: Duration::from_secs(4),
            candidate_kafka_trip_5: Duration::from_secs(5),
            decision_duration_6: Duration::from_secs(6),
            decision_download_7: Duration::from_secs(7),
            inbox_8: Duration::from_secs(8),
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
        assert_eq!(Percentile::compute(&data, 50, "mcs", &Timeline::get_total_mcs).value, 5_000_000_f32);
    }

    #[test]
    fn empty_data_should_compute_to_zero() {
        assert_eq!(Percentile::compute(&Vec::<Timeline>::new(), 50, "ms", &Timeline::get_total_mcs).value, 0_f32);
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

        let p_set = PercentileSet::new(&mut [tx1, tx3, tx2, tx5, tx4, tx6, tx8, tx7, tx10, tx9], Timeline::get_total_mcs, |t| {
            t.total.as_millis()
        });

        assert_eq!(p_set.p50.value, 5_000_000_f32);
        assert_eq!(p_set.p75.value, 8_000_000_f32);
        assert_eq!(p_set.p90.value, 9_000_000_f32);
        assert_eq!(p_set.p95.value, 10_000_000_f32);
        assert_eq!(p_set.p99.value, 10_000_000_f32);
    }

    #[test]
    fn should_convert_spans_to_ms() {
        let tx = tx("id1".to_string(), 36);

        assert_eq!(tx.get_total_mcs(), 36_000_000_f32);
        assert_eq!(tx.get_outbox_mcs(), 1_000_000_f32);
        assert_eq!(tx.get_enqueing_mcs(), 2_000_000_f32);
        assert_eq!(tx.get_publish_taks_spawn_mcs(), 3_000_000_f32);
        assert_eq!(tx.get_candidate_publish_mcs(), 4_000_000_f32);
        assert_eq!(tx.get_candidate_kafka_trip_mcs(), 5_000_000_f32);
        assert_eq!(tx.get_decision_duration_mcs(), 6_000_000_f32);
        assert_eq!(tx.get_decision_download_mcs(), 7_000_000_f32);
        assert_eq!(tx.get_inbox_mcs(), 8_000_000_f32);
    }

    // total: Duration::from_secs(total_sec),
}
// $coverage:ignore-end
