use std::{cmp::max, time::Duration};

use time::OffsetDateTime;
use tokio::sync::mpsc::{self, error::TrySendError};

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
    pub max_retry_attempts: Option<u32>,
}

impl Default for TrySendWithRetryConfig {
    fn default() -> Self {
        Self {
            max_sleep_ms: 30,
            max_retry_duration_ms: Some(1_000),
            max_retry_attempts: Some(5),
        }
    }
}

pub async fn try_send_with_retry<T: Clone>(tx: &mpsc::Sender<T>, message: T, config: TrySendWithRetryConfig) -> Result<(), TrySendError<T>> {
    let abort_time = config
        .max_retry_duration_ms
        .map(|max_duration_ms| OffsetDateTime::now_utc().unix_timestamp_nanos() + (max_duration_ms * 1_000_000) as i128);
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
                if let Some(max_attempts) = config.max_retry_attempts {
                    if counter >= max_attempts {
                        break Err(TrySendError::Full(message));
                    }
                }
                // If reached max allowed duration, break.
                if let Some(stop_ns) = abort_time {
                    if OffsetDateTime::now_utc().unix_timestamp_nanos() >= stop_ns {
                        break Err(TrySendError::Full(message));
                    }
                }

                tokio::time::sleep(Duration::from_millis(sleep_time as u64)).await;
            }
            Err(e) => break Err(e),
        }
    }
}
