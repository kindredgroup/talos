use rand::Rng;
use std::time::Duration;

#[derive(Clone)]
pub struct DelayController {
    pub total_sleep_time: u128,
    multiplier: u32,
    min_sleep_ms: u32,
    max_sleep_ms: u32,
}

// TODO: move me into cohort_sdk package
impl DelayController {
    pub fn new(min_sleep_ms: u32, max_sleep_ms: u32) -> Self {
        Self {
            multiplier: 1,
            min_sleep_ms,
            max_sleep_ms,
            total_sleep_time: 0,
        }
    }

    pub async fn sleep(&mut self) {
        let step_ms = self.min_sleep_ms;
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
            let mut rnd = rand::rng();
            rnd.random_range(m..=m * 2)
        };

        let delay_ms = std::cmp::min(self.max_sleep_ms, m + add);
        tokio::time::sleep(Duration::from_millis(delay_ms as u64)).await;
        self.total_sleep_time += delay_ms as u128;
    }
}
