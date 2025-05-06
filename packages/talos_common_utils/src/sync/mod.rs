use std::{cmp::max, time::Duration};

use tokio::{
    sync::mpsc::{self, error::TrySendError},
    time::Instant,
};

/// Configuration for attempting to retry sending message via an tokio::mpsc::Sender
pub struct TrySendWithRetryConfig {
    /// The max sleep time in ms between each retry.
    /// - Defaults to 30ms.
    pub max_sleep_ms: u32,
    /// Overall max duration to attempt retries.
    /// - Use `None` to disable this check.
    /// - Defaults to 1 seconds.
    pub max_retry_duration_ms: Option<u32>,
    /// Overall number of retries.
    /// - Use `None` to disable this check.
    /// - Defaults to 5 retries.
    pub max_retry_attemptes: Option<u32>,
}

impl Default for TrySendWithRetryConfig {
    fn default() -> Self {
        Self {
            max_sleep_ms: 30,
            max_retry_duration_ms: Some(1_000),
            max_retry_attemptes: Some(5),
        }
    }
}

pub async fn try_send_with_retry<T: Clone>(tx: &mpsc::Sender<T>, message: T, config: TrySendWithRetryConfig) -> Result<(), TrySendError<T>> {
    let start = Instant::now();
    let mut counter = 0;

    loop {
        counter += 1;
        match tx.try_send(message.clone()) {
            Ok(_) => break Ok(()),
            Err(TrySendError::Full(_)) => {
                let sleep_time = max(
                    counter * 5, // 5ms
                    config.max_sleep_ms,
                );

                // If reached max attempts, break.
                if let Some(max_attempts) = config.max_retry_attemptes {
                    if counter >= max_attempts {
                        break Err(TrySendError::Full(message));
                    }
                }
                // If reached max allowed duration, break.
                if let Some(max_duration_ms) = config.max_retry_duration_ms {
                    if start.elapsed().as_millis() + sleep_time as u128 >= max_duration_ms as u128 {
                        break Err(TrySendError::Full(message));
                    }
                }

                tokio::time::sleep(Duration::from_millis(sleep_time as u64)).await;
            }
            Err(e) => break Err(e),
        }
    }
}
