use rand::Rng;
use std::time::Duration;

#[derive(Clone)]
pub struct DelayController {
    pub total_sleep_time: u128,
    multiplier: u64,
    max_sleep_ms: u64,
}

// TODO: move me into cohort_sdk package
impl DelayController {
    pub fn new(max_sleep_ms: u64) -> Self {
        Self {
            multiplier: 1,
            max_sleep_ms,
            total_sleep_time: 0,
        }
    }

    pub async fn sleep(&mut self) {
        let step_ms = 20;
        if self.multiplier > 64 {
            self.multiplier = 1;
        }

        let m = if self.multiplier == 1 {
            self.multiplier * step_ms
        } else {
            self.multiplier * 2 * step_ms
        };

        self.multiplier *= 2;

        let add = {
            let mut rnd = rand::thread_rng();
            rnd.gen_range(m..=m * 2)
        };

        let delay_ms = std::cmp::min(self.max_sleep_ms, m + add);
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        self.total_sleep_time += delay_ms as u128;
    }
}
