use tokio::sync::mpsc;

use log::{error, info};
// use futures_executor::block_on;
use rdkafka::{
    consumer::{ConsumerContext, Rebalance},
    ClientContext,
};
use talos_common_utils::{
    sync::{try_send_with_retry, TrySendWithRetryConfig},
    ResetVariantTrait,
};

/// Replicators's consumer context used to detect a rebalance and update the snapshot.
pub struct ReplicatorConsumerContext<T>
where
    T: ResetVariantTrait + Send + Sync,
{
    pub topic: String,
    pub message_channel_tx: mpsc::Sender<T>,
}

impl<T> ClientContext for ReplicatorConsumerContext<T> where T: ResetVariantTrait + Send + Sync {}
impl<T> ConsumerContext for ReplicatorConsumerContext<T>
where
    T: ResetVariantTrait + Clone + Send + Sync + 'static,
{
    fn post_rebalance(&self, rebalance: &rdkafka::consumer::Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(partitions) => {
                info!("Rebalance complete and partition of certification topic: {:?} is assigned", self.topic);
                let tpl_vec = partitions.elements_for_topic(&self.topic);
                if !tpl_vec.is_empty() {
                    tokio::task::spawn({
                        let message_channel_tx = self.message_channel_tx.clone();
                        async move {
                            if let Err(err) = try_send_with_retry(&message_channel_tx, T::get_reset_variant(), TrySendWithRetryConfig::default()).await {
                                error!("Failed to send reset message due to error {err:?}");
                            };
                        }
                    });
                }
            }
            Rebalance::Revoke(_partitions) => {}
            Rebalance::Error(e) => error!("Rebalance error: {}", e),
        }
    }
}
