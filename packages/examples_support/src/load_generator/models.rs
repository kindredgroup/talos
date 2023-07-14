use std::time::Duration;

use time::OffsetDateTime;

pub(crate) struct Progress {
    pub(crate) delta: f32,
    pub(crate) current_rate: f32,
}

impl Progress {
    pub(crate) fn get(started_at: i128, target_rate: f32, generated_count: u64) -> Self {
        let now = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let elapsed_sec = Duration::from_nanos((now - started_at) as u64).as_secs_f32();
        let expected_samples = elapsed_sec * target_rate;
        let delta = expected_samples - generated_count as f32;

        Self {
            delta,
            current_rate: (generated_count as f32 / elapsed_sec),
        }
    }
}

#[derive(Clone)]
pub enum StopType {
    LimitExecutionDuration { run_duration: Duration },
    LimitGeneratedTransactions { count: u64 },
}

pub trait Generator<T> {
    fn generate(&mut self) -> T;
}
