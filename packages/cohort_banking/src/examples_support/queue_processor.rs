use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use opentelemetry_api::metrics::{Meter, Unit};
use tokio::task::JoinHandle;

use async_trait::async_trait;

pub struct QueueProcessor {}

#[async_trait]
pub trait Handler<T: Send + Sync + 'static>: Sync + Send {
    async fn handle(&self, item: T) -> Result<(), String>;
}

impl QueueProcessor {
    pub async fn process<T: Send + Sync + 'static, H: Handler<T> + 'static>(
        queue: Arc<async_channel::Receiver<(T, f64)>>,
        meter: Arc<Meter>,
        threads: u64,
        item_handler: Arc<H>,
    ) -> Vec<JoinHandle<()>> {
        let mut tasks = Vec::<JoinHandle<()>>::new();

        for thread_number in 1..=threads {
            let queue_ref = Arc::clone(&queue);
            let item_handler = Arc::clone(&item_handler);
            let meter = Arc::clone(&meter);

            let task_h: JoinHandle<()> = tokio::spawn(async move {
                let counter = Arc::new(meter.u64_counter("metric_count").with_unit(Unit::new("tx")).init());
                let histogram = Arc::new(meter.f64_histogram("metric_candidate_roundtrip").with_unit(Unit::new("ms")).init());
                let histogram_sys = Arc::new(meter.f64_histogram("metric_candidate_roundtrip_sys").with_unit(Unit::new("ms")).init());
                let histogram_throughput = Arc::new(meter.f64_histogram("metric_throughput").with_unit(Unit::new("ms")).init());

                let mut handled_count = 0;
                let mut errors_count = 0;

                loop {
                    let histogram_ref = Arc::clone(&histogram);
                    let histogram_sys_ref = Arc::clone(&histogram_sys);
                    let histogram_throughput_ref = Arc::clone(&histogram_throughput);
                    let counter_ref = Arc::clone(&counter);

                    match queue_ref.recv().await {
                        Err(_) => {
                            errors_count += 1;
                            break;
                        }
                        Ok((item, scheduled_at_ms)) => {
                            handled_count += 1;

                            let started_at_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as f64 / 1_000_000_f64;
                            let result = item_handler.handle(item).await;

                            let processing_finished_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as f64 / 1_000_000_f64;
                            tokio::spawn(async move {
                                histogram_ref.record((processing_finished_ms - started_at_ms) * 100.0, &[]);
                                histogram_sys_ref.record((processing_finished_ms - scheduled_at_ms) * 10.0, &[]);

                                // Record start and stop times of each transaction.
                                // This histogram will track min and max values, giving us the total duration of the test, excluding test specific code.
                                // The count value in this histogram will be 2x inflated, which will need to be accounted for.
                                histogram_throughput_ref.record(scheduled_at_ms, &[]);
                                histogram_throughput_ref.record(processing_finished_ms, &[]);

                                counter_ref.add(1, &[]);
                            });

                            if let Err(e) = result {
                                log::warn!(
                                    "Thread {} cannot process more requests. Error handling item: {}. Processed items: {}",
                                    thread_number,
                                    e,
                                    handled_count
                                );
                                break;
                            }
                        }
                    }
                }

                log::debug!(
                    "Thread {:>2} stopped. Processed items: {}. Errors: {}",
                    thread_number,
                    handled_count,
                    errors_count
                );
            });
            tasks.push(task_h);
        }

        tasks
    }
}
