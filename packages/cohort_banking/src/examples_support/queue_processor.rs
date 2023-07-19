use std::{sync::Arc, time::Instant};

use opentelemetry_api::{
    metrics::{Meter, Unit},
    Context,
};
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
    ) -> Vec<JoinHandle<()>> {
        let item_handler = Arc::new(item_handler);
        let mut tasks = Vec::<JoinHandle<()>>::new();

        for thread_number in 1..=threads {
            let queue_ref = Arc::clone(&queue);
            let item_handler = Arc::clone(&item_handler);
            let meter = Arc::clone(&meter);
            let task_h: JoinHandle<()> = tokio::spawn(async move {
                let histogram = Arc::new(meter.f64_histogram("duration").with_unit(Unit::new("ms")).init());
                let counter = Arc::new(meter.f64_counter("count").with_unit(Unit::new("tx")).init());

                let mut handled_count = 0;
                loop {
                    let histogram_ref = Arc::clone(&histogram);
                    let counter_ref = Arc::clone(&counter);

                    match queue_ref.recv().await {
                        Err(_) => break,
                        Ok(item) => {
                            handled_count += 1;
                            let s = Instant::now();
                            let result = item_handler.handle(item).await;
                            let v = s.elapsed().as_nanos() as f64 / 1_000_000_f64;

                            let _ = tokio::spawn(async move {
                                let ctx = &Context::current();
                                histogram_ref.record(ctx, v, &[]);
                                counter_ref.add(ctx, 1.0, &[]);
                            })
                            .await;

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

                log::debug!("Thread {:>2} stopped. Processed items: {}.", thread_number, handled_count);
            });
            tasks.push(task_h);
        }

        tasks
    }
}
