use time::OffsetDateTime;

#[derive(Debug, Clone)]
pub struct MicroMetrics {
    time_unit_ratio: f32,
    pub is_enabled: bool,
    pub count: f32,
    pub min: f32,
    pub max: f32,
    pub avg: f32,
    pub rate_avg: f32,
    pub duration: f32,
    pub printed_at: i128,
    pub started_at: i128,

    pub clock_started_at: Option<i128>,
}

impl MicroMetrics {
    pub fn new(time_unit_ratio: f32, is_enabled: bool) -> Self {
        Self {
            is_enabled,
            time_unit_ratio,
            count: 0.0,
            min: f32::MAX,
            max: 0.0,
            avg: 0.0,
            rate_avg: 0.0,
            duration: 0.0,
            printed_at: 0,
            started_at: 0,
            clock_started_at: None,
        }
    }

    pub fn start(&mut self) {
        self.clock_start()
    }

    /// Returns total duration for the entier sampling window
    pub fn clock_end(&mut self) -> i128 {
        self.count += 1.0;
        let end = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let duration = (end - self.clock_started_at.expect("Sample time did not start")) as f32 / self.time_unit_ratio;
        self.duration += duration;

        self.min = if self.min > duration { duration } else { self.min };
        self.max = if self.max < duration { duration } else { self.max };
        self.avg = self.duration / self.count;
        self.rate_avg = self.count / self.duration * if self.time_unit_ratio == 1_000_000_f32 { 1_000_f32 } else { 1_f32 };

        self.clock_started_at = None;

        let since = if self.printed_at == 0 { self.started_at } else { self.printed_at };
        end - since
    }

    pub fn clock_start(&mut self) {
        let now = OffsetDateTime::now_utc().unix_timestamp_nanos();
        if self.clock_started_at.is_none() {
            self.clock_started_at = Some(now);
        }
        if self.started_at == 0 {
            self.started_at = now;
        }
    }

    pub fn sample_end(&mut self) {
        self.printed_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
    }

    pub fn print_with_custom_duration(&self, time_unit: &str, custom_duration: i128) -> String {
        let custom_duration = custom_duration as f32 / self.time_unit_ratio;
        format!(
            "count: {}, min: {:.6}{}, max: {:.6}{}, avg: {:.6}{}, rate avg: {:.6} tps, duration: {:.6}{}",
            self.count, self.min, time_unit, self.max, time_unit, self.avg, time_unit, self.rate_avg, custom_duration, time_unit,
        )
    }

    pub fn print(&self, time_unit: &str) -> String {
        format!(
            "count: {}, min: {:.6}{}, max: {:.6}{}, avg: {:.6}{}, rate avg: {:.6} tps, duration: {:.6}{}",
            self.count, self.min, time_unit, self.max, time_unit, self.avg, time_unit, self.rate_avg, self.duration, time_unit,
        )
    }
}

#[derive(Debug, Clone)]
pub struct MinMax {
    pub sum: i128,
    pub min: i128,
    pub max: i128,
    pub count: i128,
}

impl MinMax {
    pub fn merge(&mut self, value: MinMax) {
        if self.min > value.min {
            self.min = value.min;
        }
        if self.max < value.max && value.max < i128::MAX {
            self.max = value.max;
        }
        self.sum += value.sum;
        self.count += value.count;
    }

    pub fn add(&mut self, value: i128) {
        if self.min > value {
            self.min = value;
        }
        if self.max < value && value < i128::MAX {
            self.max = value;
        }
        self.count += 1;
        self.sum += value;
    }

    pub fn reset(&mut self) {
        self.sum = 0;
        self.min = i128::MAX;
        self.max = 0;
        self.count = 0;
    }
}

impl Default for MinMax {
    fn default() -> Self {
        MinMax {
            min: i128::MAX,
            max: 0,
            count: 0,
            sum: 0,
        }
    }
}
