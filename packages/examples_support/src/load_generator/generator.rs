use std::{sync::Arc, time::Duration};

use async_channel::Sender;
use time::OffsetDateTime;

use crate::load_generator::models::{Progress, StopType};

pub struct ControlledRateLoadGenerator {}

impl ControlledRateLoadGenerator {
    pub async fn generate<T>(stop_type: StopType, target_rate: f32, fn_item_factory: &impl Fn() -> T, tx_output: Arc<Sender<T>>) -> Result<(), String> {
        let started_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
        if let StopType::LimitExecutionDuration { run_duration } = stop_type {
            let stop_at = started_at + run_duration.as_nanos() as i128;
            let stop_at_time = OffsetDateTime::from_unix_timestamp_nanos(stop_at);
            log::warn!("Load generator will stop at: {:?}", stop_at_time);
        }

        let mut generated_count = 0_u64;

        let check_every = 3_u64;
        let mut last_progress_print = started_at;
        loop {
            // rate controller
            if generated_count % check_every == 0 {
                let progress = Progress::get(started_at, target_rate, generated_count);

                if progress.delta < 0.0 {
                    // too fast, need to slow down
                    let delay_sec = progress.delta / target_rate * -1_f32;
                    tokio::time::sleep(tokio::time::Duration::from_secs_f32(delay_sec)).await;
                }
            }

            match stop_type {
                StopType::LimitGeneratedTransactions { count: total_count } => {
                    // 5% of total count
                    let mut print_frequency = total_count as f32 / 5.0;
                    if print_frequency < 10.0 {
                        print_frequency = total_count as f32 / 2.0;
                    }

                    if generated_count > 0 && generated_count as f32 % print_frequency == 0.0 {
                        let progress = Progress::get(started_at, target_rate, generated_count);
                        log::warn!(
                            "Limit-count: generated: {:>7}, target: {:>5} tps, current: {:>5.2} tps",
                            generated_count,
                            target_rate,
                            progress.current_rate
                        );
                    }
                }

                StopType::LimitExecutionDuration { run_duration } => {
                    // 5% of total duration
                    let print_every_ms = (run_duration.as_secs_f32() * 1_000_f32 / 5.0) as u128;
                    let now = OffsetDateTime::now_utc().unix_timestamp_nanos();
                    let elapsed_ms = Duration::from_nanos((now - last_progress_print) as u64).as_millis();
                    if elapsed_ms >= print_every_ms {
                        let progress = Progress::get(started_at, target_rate, generated_count);
                        log::warn!(
                            "Limit-time: generated: {:>7}, target: {:>4} tps, current: {:>5.2} tps",
                            generated_count,
                            target_rate,
                            progress.current_rate
                        );
                        last_progress_print = now;
                    }
                }
            }

            let new_item: T = fn_item_factory();
            let _ = tx_output.send(new_item).await;
            generated_count += 1;

            let now = OffsetDateTime::now_utc().unix_timestamp_nanos();
            let should_stop = match stop_type {
                StopType::LimitExecutionDuration { run_duration } => {
                    let stop_at = started_at + (run_duration.as_secs_f32() * 1_000_000_000_f32) as i128;
                    stop_at <= now
                }

                StopType::LimitGeneratedTransactions { count } => count == generated_count,
            };

            if should_stop {
                break;
            }
        }

        let now = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let duration = Duration::from_nanos((now - started_at) as u64).as_secs_f32();
        log::warn!(
            "\nLoad generator has finished:\n  Run duration:   {:.2} sec.\n  Generated:      {} transactions\n  Effective rate: {:.2} tps",
            duration,
            generated_count,
            (generated_count as f32 / duration)
        );

        Ok(())
    }
}
