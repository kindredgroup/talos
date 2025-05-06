use futures_executor::block_on;
use log::{error, info};
use rdkafka::{
    consumer::{ConsumerContext, Rebalance},
    ClientContext,
};
use talos_certifier::{model::CandidateMessage, ChannelMessage};
use talos_common_utils::{
    sync::{try_send_with_retry, TrySendWithRetryConfig},
    ResetVariantTrait,
};
use tokio::sync::mpsc;

/// Certifier's consumer context used to detect a rebalance and send a `ChannelMessage::Reset` to reset the suffix in `certifier service`.
pub struct CertifierConsumerContext<T>
where
    T: ResetVariantTrait + Send + Sync,
{
    pub topic: String,
    pub message_channel_tx: mpsc::Sender<T>,
}

impl<T> ClientContext for CertifierConsumerContext<T> where T: ResetVariantTrait + Send + Sync {}
impl<T> ConsumerContext for CertifierConsumerContext<T>
where
    T: ResetVariantTrait + Clone + Send + Sync,
{
    fn post_rebalance(&self, rebalance: &rdkafka::consumer::Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(partitions) => {
                let k = partitions.elements_for_topic(&self.topic);
                if !k.is_empty() {
                    info!("Rebalance complete and partition of certification topic: {:?} is assigned", self.topic);
                }
            }
            Rebalance::Revoke(partitions) => {
                let tpl_vec = partitions.elements_for_topic(&self.topic);
                if !tpl_vec.is_empty() {
                    if let Err(err) = block_on(try_send_with_retry(
                        &self.message_channel_tx,
                        T::get_reset_variant(),
                        TrySendWithRetryConfig::default(),
                    )) {
                        error!("Failed to send reset message due to error {err:?}");
                    };
                }
            }
            Rebalance::Error(e) => error!("Rebalance error: {}", e),
        }
    }
}
