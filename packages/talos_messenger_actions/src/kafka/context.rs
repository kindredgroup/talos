use futures_executor::block_on;
use log::info;
use rdkafka::{producer::ProducerContext, ClientContext, Message};
use talos_messenger_core::core::MessengerChannelFeedback;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct MessengerProducerDeliveryOpaque {
    pub version: u64,
    pub total_publish_count: u32,
}

pub struct MessengerProducerContext {
    pub tx_feedback_channel: mpsc::Sender<MessengerChannelFeedback>,
}

impl ClientContext for MessengerProducerContext {}
impl ProducerContext for MessengerProducerContext {
    type DeliveryOpaque = Box<MessengerProducerDeliveryOpaque>;

    fn delivery(&self, delivery_result: &rdkafka::producer::DeliveryResult<'_>, delivery_opaque: Self::DeliveryOpaque) {
        let result = delivery_result.as_ref();

        let version = delivery_opaque.version;

        match result {
            Ok(msg) => {
                info!("Message {:?} {:?}", msg.key(), msg.offset());
                // Safe to ignore error check, as error occurs only if receiver is closed or dropped, which would happen if the thread receving has errored. In such a scenario, the publisher thread would also shutdown.
                let _ = block_on(self.tx_feedback_channel.send(MessengerChannelFeedback::Success(
                    version,
                    "kafka".to_string(),
                    delivery_opaque.total_publish_count,
                )));
            }
            Err(err) => {
                // Safe to ignore error check, as error occurs only if receiver is closed or dropped, which would happen if the thread receving has errored. In such a scenario, the publisher thread would also shutdown.
                let _ = block_on(self.tx_feedback_channel.send(MessengerChannelFeedback::Error(version, err.0.to_string())));
            }
        }
    }
}
