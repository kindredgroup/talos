use std::{
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use metrics::{model::MinMax, opentel::global};
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
        queue: Arc<async_channel::Receiver<T>>,
        meter: Arc<Meter>,
        threads: u64,
        item_handler: Arc<H>,
    ) -> Vec<JoinHandle<MinMax>> {
        let item_handler = Arc::new(item_handler);
        let mut tasks = Vec::<JoinHandle<MinMax>>::new();

        for thread_number in 1..=threads {
            let queue_ref = Arc::clone(&queue);
            let item_handler = Arc::clone(&item_handler);
            let meter = Arc::clone(&meter);
            let task_h: JoinHandle<MinMax> = tokio::spawn(async move {
                let mut timeline = MinMax::default();
                let histogram = Arc::new(meter.f64_histogram("metric_duration").with_unit(Unit::new("ms")).init());
                let counter = Arc::new(meter.u64_counter("metric_count").with_unit(Unit::new("tx")).init());

                let mut handled_count = 0;

                loop {
                    let histogram_ref = Arc::clone(&histogram);
                    match queue_ref.recv().await {
                        Err(_) => break,
                        Ok(item) => {
                            timeline.add(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i128);
                            handled_count += 1;
                            let span_dur_start = Instant::now();
                            let result = item_handler.handle(item).await;
                            let span_dur_value = span_dur_start.elapsed().as_nanos() as f64 / 1_000_000_f64;
                            tokio::spawn(async move {
                                let scale = global::scaling_config().get_scale_factor("metric_duration");
                                histogram_ref.record(span_dur_value * scale as f64, &[]);
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

                timeline.add(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i128);

                tokio::spawn(async move {
                    counter.add(handled_count, &[]);
                });
                log::debug!("Thread {:>2} stopped. Processed items: {}.", thread_number, handled_count);

                timeline
            });
            tasks.push(task_h);
        }

        tasks
    }
}