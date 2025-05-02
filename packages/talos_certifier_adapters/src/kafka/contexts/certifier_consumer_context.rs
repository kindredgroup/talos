use futures_executor::block_on;
use log::{error, info};
use rdkafka::{
    consumer::{ConsumerContext, Rebalance},
    ClientContext,
};
use talos_certifier::{model::CandidateMessage, ChannelMessage};
use tokio::sync::mpsc;

/// Certifier's consumer context used to detect a rebalance and send a `ChannelMessage::Reset` to reset the suffix in `certifier service`.
pub struct CertifierConsumerContext {
    pub topic: String,
    pub message_channel_tx: mpsc::Sender<ChannelMessage<CandidateMessage>>,
}

impl ClientContext for CertifierConsumerContext {}
impl ConsumerContext for CertifierConsumerContext {
    fn post_rebalance(&self, rebalance: &rdkafka::consumer::Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(partitions) => {
                let k = partitions.elements_for_topic(&self.topic);
                if !k.is_empty() {
                    info!("Assigned to partition of certification topic: {:?}", self.topic);
                }
            }
            Rebalance::Revoke(partitions) => {
                let k = partitions.elements_for_topic(&self.topic);
                if !k.is_empty() {
                    info!("Sending reset message due to partition revoked on certification topic {}", self.topic);
                    if let Err(_error) = block_on(self.message_channel_tx.send(ChannelMessage::Reset)) {
                        error!("Failed to send reset message");
                    };
                }
            }
            Rebalance::Error(e) => error!("Rebalance error: {}", e),
        }
    }
}
