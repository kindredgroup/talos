use std::{collections::HashMap, time::Duration};

use metrics::model::MinMax;

#[derive(Debug, Clone)]
pub struct Span {
    pub started: i128,
    pub ended: i128,
}

impl Span {
    pub fn new(started: i128, ended: i128) -> Self {
        Self { started, ended }
    }

    pub fn duration(&self) -> i128 {
        self.ended - self.started
    }
}

pub struct TxExecSpans {
    pub span1_get_accounts: Span,
    pub span2_get_snap_ver: Span,
    pub span3_certify: Span,
    pub span4_wait_for_safepoint: Span,
    pub span5_install: Span,
}

impl Default for TxExecSpans {
    fn default() -> Self {
        TxExecSpans {
            span1_get_accounts: Span::new(0, 0),
            span2_get_snap_ver: Span::new(0, 0),
            span3_certify: Span::new(0, 0),
            span4_wait_for_safepoint: Span::new(0, 0),
            span5_install: Span::new(0, 0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Stats {
    pub min_started_at: i128,
    pub max_finished_at: i128,
    pub aborts: u64,
    pub isolation_errors: u64,
    pub retry_min_max: MinMax,
    pub retry_count: u64,
    pub validation_errors: u64,
    pub sleep_time: u128,

    pub getaccounts: MinMax,
    pub getsnap: MinMax,
    pub certify: MinMax,
    pub waiting: MinMax,
    pub installing: MinMax,

    pub duration_min_max: MinMax,

    pub giveup_count: u128,
    pub total_count: u128,

    pub exceptions: u128,
    pub threading_errors: u128,

    pub durations: Vec<i128>,
}

impl Default for Stats {
    fn default() -> Self {
        Self::new()
    }
}

impl Stats {
    pub fn new() -> Stats {
        Stats {
            min_started_at: i128::MAX,
            max_finished_at: 0,
            aborts: 0,
            isolation_errors: 0,
            retry_count: 0,
            retry_min_max: MinMax::default(),
            validation_errors: 0,
            sleep_time: 0,
            duration_min_max: MinMax::default(),
            giveup_count: 0,
            total_count: 0,
            getaccounts: MinMax::default(),
            getsnap: MinMax::default(),
            certify: MinMax::default(),
            waiting: MinMax::default(),
            installing: MinMax::default(),

            exceptions: 0,
            threading_errors: 0,

            durations: Vec::new(),
        }
    }

    pub fn inc_retry_count(&mut self) {
        self.retry_count += 1;
        self.retry_min_max.add(self.retry_count as i128);
    }

    pub fn on_tx_finished(&mut self, started_at: i128, finished_at: i128) {
        if self.min_started_at > started_at {
            self.min_started_at = started_at;
        }
        if self.max_finished_at < finished_at {
            self.max_finished_at = finished_at;
        }
        self.on_tx_completed(finished_at - started_at);
    }

    fn on_tx_completed(&mut self, duration_nanos: i128) {
        self.duration_min_max.add(duration_nanos);
        self.durations.push(duration_nanos);
    }

    pub fn merge(&mut self, stats: Stats) {
        if self.min_started_at > stats.min_started_at {
            self.min_started_at = stats.min_started_at;
        }
        if self.max_finished_at < stats.max_finished_at {
            self.max_finished_at = stats.max_finished_at;
        }

        self.aborts += stats.aborts;
        self.isolation_errors += stats.isolation_errors;
        self.retry_count += stats.retry_count;
        self.retry_min_max.add(stats.retry_min_max.min);
        self.retry_min_max.add(stats.retry_min_max.max);

        self.validation_errors += stats.validation_errors;
        self.sleep_time += stats.sleep_time;
        self.getaccounts.merge(stats.getaccounts);
        self.getsnap.merge(stats.getsnap);
        self.certify.merge(stats.certify);
        self.waiting.merge(stats.waiting);
        self.installing.merge(stats.installing);

        self.duration_min_max.add(stats.duration_min_max.min);
        self.duration_min_max.add(stats.duration_min_max.max);

        self.giveup_count += stats.giveup_count;
        self.total_count += stats.total_count;
        for d in stats.durations {
            self.durations.push(d);
        }
    }

    pub fn generate_report(&mut self, threads: u64, max_retry: u64) -> String {
        let p_lables: Vec<f32> = vec![50_f32, 75_f32, 90_f32, 95_f32, 99_f32, 99.9];
        let p_durations = Self::compute_percentiles::<i128>(&p_lables, &mut self.durations);

        let duration_sec = Duration::from_nanos((self.max_finished_at - self.min_started_at) as u64).as_secs_f32();
        let mut report: String = "".into();
        report += "\n------------------------------------------";
        report += &format!("\nSet size    : {}", (self.total_count / threads as u128) as u64);
        report += " - candidates per thread";
        report += &format!("\nThreads     : {}", threads);
        report += &format!("\nTotal time  : {:.3} (sec)", duration_sec);
        report += &format!("\nTransactions: {}", self.total_count);
        report += &format!("\nRetries     : {}", self.retry_count);
        report += &format!("\n  min, max  : {}, {}", self.retry_min_max.min, self.retry_min_max.max);
        report += &format!("\nGiven up    : {}", self.giveup_count);
        report += &format!(" - used up all retry attempts ({})", max_retry);
        report += "\n\nThroughput (tps)";
        report += &format!("\n  Client view     : {:.3}", (self.total_count as f32 / duration_sec));
        report += " - observed by end user, this excludes retries";
        report += &format!(
            "\n  System          : {:.3}",
            ((self.retry_count as u128 + self.total_count) as f32 / duration_sec)
        );
        report += " - produced by system, this includes retries";
        report += &format!(
            "\n  System projected: {:.3}",
            ((self.retry_count as u128 + self.total_count) as f32 / (duration_sec - (self.sleep_time as f32 / threads as f32 / 1000.0)))
        );
        report += " - produced by system excluding time spent sleeping";
        report += "\n\nDurations";
        report += &format!(
            "\n  Tx min    : {:.3} (sec)",
            Duration::from_nanos(self.duration_min_max.min as u64).as_secs_f64()
        );
        report += &format!(
            "\n  Tx max    : {:.3} (sec)",
            Duration::from_nanos(self.duration_min_max.max as u64).as_secs_f64()
        );
        report += " - candidate roundtrip\n";
        for p_label in p_lables {
            report += &format!(
                "\n     p{:<6}: {}",
                p_label,
                p_durations
                    .get(&p_label.to_string())
                    .map_or("".to_string(), |v| format!("{:.3} (sec)", (*v) as f32 / 1_000_000_000.0))
            );
        }

        report += &format!(
            "\n\n  Sleeps    : {:.3} (sec avg. per retry)",
            self.sleep_time as f32 / 1000.0 / self.retry_count as f32
        );
        report += " - time wasted on sleeps before retrying";
        report += &format!(
            "\n  Waited    : {:.3} (sec avg. per thread) - initiator waited for safepoint",
            (Duration::from_nanos(self.waiting.sum as u64).as_secs_f64()) / threads as f64
        );
        report += &format!("\n            : {:.3} (sec min)", (Duration::from_nanos(self.waiting.min as u64).as_secs_f64()));
        report += &format!("\n            : {:.3} (sec max)", (Duration::from_nanos(self.waiting.max as u64).as_secs_f64()));
        report += "\n\nErrors";

        report += &format!("\n  Talos aborts: {}", self.aborts);
        report += &format!("\n  Validations : {}", self.validation_errors);
        report += &format!("\n  DB isolation: {}", self.isolation_errors);
        report += " - DB rollbacks caused by tx isolation conflicts";
        report += &format!("\n  Exceptions  : {}", self.exceptions);
        report += &format!("\n  Threading   : {}", self.threading_errors);
        report += " - Tokio threading errors";

        report += "\n------------------------------------------";

        report
    }

    pub fn compute_percentiles<T: Ord + Clone>(p_list: &Vec<f32>, values: &mut Vec<T>) -> HashMap<String, T> {
        let mut result = HashMap::<String, T>::new();
        values.sort();
        let count = values.len() as f32;
        for p_ref in p_list {
            let p = *p_ref;
            let p_index = (count * p / 100.0) as usize - 1;
            result.insert(p.to_string(), values[p_index].clone());
        }
        result
    }
}
