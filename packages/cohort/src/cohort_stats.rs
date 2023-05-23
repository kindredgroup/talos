pub struct Stats {
    pub aborts: u64,
    pub isolation_errors: u64,
    pub retry_min_max: Option<(u64, u64)>,
    pub retry_count: u64,
    pub validation_errors: u64,
    pub sleep_time: u128,

    pub duration_tx_min: i128,
    pub duration_tx_max: i128,

    pub duration_set_min: i128,
    pub duration_set_max: i128,

    pub giveup_count: u128,
    pub total_count: u128,

    pub exceptions: u128,
    pub threading_errors: u128,
}

impl Default for Stats {
    fn default() -> Self {
        Self::new()
    }
}

impl Stats {
    pub fn new() -> Stats {
        Stats {
            aborts: 0,
            isolation_errors: 0,
            retry_count: 0,
            retry_min_max: None,
            validation_errors: 0,
            sleep_time: 0,
            duration_tx_min: i128::MAX,
            duration_tx_max: 0,
            duration_set_min: i128::MAX,
            duration_set_max: 0,
            giveup_count: 0,
            total_count: 0,

            exceptions: 0,
            threading_errors: 0,
        }
    }

    pub fn on_tx_completed(&mut self, duration_nanos: i128) {
        if self.duration_tx_min > duration_nanos {
            self.duration_tx_min = duration_nanos;
        }
        if self.duration_tx_max < duration_nanos {
            self.duration_tx_max = duration_nanos;
        }
    }

    pub fn on_set_completed(&mut self, duration_nanos: i128) {
        if self.duration_set_min > duration_nanos {
            self.duration_set_min = duration_nanos;
        }
        if self.duration_set_max < duration_nanos {
            self.duration_set_max = duration_nanos;
        }
    }

    pub fn merge(&mut self, stats: Stats) {
        self.aborts += stats.aborts;
        self.isolation_errors += stats.isolation_errors;
        self.retry_count += stats.retry_count;
        let (mut min, mut max) = if self.retry_min_max.is_none() {
            (u64::MAX, 0)
        } else {
            self.retry_min_max.unwrap()
        };

        let other = if stats.retry_min_max.is_none() {
            (stats.retry_count, stats.retry_count)
        } else {
            stats.retry_min_max.unwrap()
        };

        if min > other.0 {
            min = other.0
        }
        if max < other.1 {
            max = other.1
        }

        self.retry_min_max = Some((min, max));

        self.validation_errors += stats.validation_errors;
        self.sleep_time += stats.sleep_time;

        if self.duration_tx_min > stats.duration_tx_min {
            self.duration_tx_min = stats.duration_tx_min;
        }
        if self.duration_tx_max < stats.duration_tx_max {
            self.duration_tx_max = stats.duration_tx_max;
        }
        if self.duration_set_min > stats.duration_set_min {
            self.duration_set_min = stats.duration_set_min;
        }
        if self.duration_set_max < stats.duration_set_max {
            self.duration_set_max = stats.duration_set_max;
        }

        self.giveup_count += stats.giveup_count;
        self.total_count += stats.total_count;
    }

    pub(crate) fn generate_report(&self, threads: u8, duration: f32) -> String {
        let mut report: String = "".into();
        report += "\n------------------------------------------";
        report += &format!("\nSet size    : {}", (self.total_count / threads as u128) as u64);
        report += " - candidates per thread";
        report += &format!("\nThreads     : {}", threads);
        report += &format!("\nThroughput  : {} (per sec)", (self.total_count as f32 / (duration / 1000000000.0)));
        report += &format!("\nTotal time  : {} (sec)", duration / 1000000000.0);
        report += &format!("\nTransactions: {}", self.total_count);
        report += &format!("\nRetries     : {}", self.retry_count);
        report += &format!("\n  min, max  : {:?}", self.retry_min_max.unwrap());
        report += &format!("\nGiven up    : {}", self.giveup_count);
        report += " - used up all retry attempts (50)";

        report += "\n\nDurations";
        report += &format!("\n  Tx min    : {} (sec)", self.duration_tx_min as f32 / 1000000000.0);
        report += " - candidate roundtrip";
        report += &format!("\n  Tx max    : {} (sec)", self.duration_tx_max as f32 / 1000000000.0);
        report += " - candidate roundtrip";
        report += &format!("\n  Set min   : {} (sec)", self.duration_set_min as f32 / 1000000000.0);
        report += " - CSV file processing time";
        report += &format!("\n  Set max   : {} (sec)", self.duration_set_max as f32 / 1000000000.0);
        report += " - CSV file processing time";
        report += &format!("\n  Sleeps    : {} (sec)", self.sleep_time as f32 / 1000.0);
        report += " - time wasted on sleeps before retrying";
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
}
