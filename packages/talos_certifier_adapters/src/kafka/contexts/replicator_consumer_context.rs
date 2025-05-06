use log::{error, info};
// use futures_executor::block_on;
use rdkafka::{
    consumer::{ConsumerContext, Rebalance},
    ClientContext,
};

/// Replicators's consumer context used to detect a rebalance and update the snapshot.
pub struct ReplicatorConsumerContext<F>
where
    F: Fn() -> Result<(), String> + Send + Sync + 'static,
{
    pub topic: String,
    pub rebalance_assign_callback_fn: F,
}

impl<F> ClientContext for ReplicatorConsumerContext<F> where F: Fn() -> Result<(), String> + Send + Sync + 'static {}
impl<F> ConsumerContext for ReplicatorConsumerContext<F>
where
    F: Fn() -> Result<(), String> + Send + Sync + 'static,
{
    fn post_rebalance(&self, rebalance: &rdkafka::consumer::Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(partitions) => {
                let k = partitions.elements_for_topic(&self.topic);
                if !k.is_empty() {
                    let _ = (self.rebalance_assign_callback_fn)();
                    info!("Consumer connected to a partition of certification topic: {:?}", self.topic);
                }
            }
            Rebalance::Revoke(_partitions) => {}
            Rebalance::Error(e) => error!("Rebalance error: {}", e),
        }
    }
}
