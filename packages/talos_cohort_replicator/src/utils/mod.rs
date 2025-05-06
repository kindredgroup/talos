pub(crate) mod installer_utils;
mod replicator_utils;
use std::time::Duration;

pub use replicator_utils::*;
use tokio::{
    sync::mpsc::{self, error::TrySendError},
    time::Instant,
};

/// Configuration for attempting to retry sending message via an tokio::mpsc::Sender
pub struct SendWithRetryConfig {
    pub max_duration_ms: Option<u32>,
    pub max_attemptes: Option<u32>,
}

impl Default for SendWithRetryConfig {
    fn default() -> Self {
        Self {
            max_duration_ms: Some(10_000),
            max_attemptes: Some(5),
        }
    }
}

pub async fn send_with_retry<T: Clone>(tx: &mpsc::Sender<T>, message: T, config: SendWithRetryConfig) -> Result<(), TrySendError<T>> {
    let start = Instant::now();
    let mut counter = 0;

    loop {
        counter += 1;
        match tx.try_send(message.clone()) {
            Ok(_) => break Ok(()),
            Err(TrySendError::Full(_)) => {
                let sleep_time = counter * 5; // 5 ms

                if let Some(max_attempts) = config.max_attemptes {
                    if counter >= max_attempts {
                        break Err(TrySendError::Full(message));
                    }
                }
                if let Some(max_duration_ms) = config.max_duration_ms {
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
