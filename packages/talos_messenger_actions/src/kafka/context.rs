use futures_executor::block_on;
use log::{error, info};
use rdkafka::{producer::ProducerContext, ClientContext, Message};
use talos_messenger_core::{core::MessengerChannelFeedback, errors::MessengerActionError};
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct MessengerProducerDeliveryOpaque {
    pub version: u64,
    pub total_publish_count: u32,
}

#[derive(Debug, Clone)]
pub struct MessengerProducerContext {
    pub tx_feedback_channel: mpsc::Sender<MessengerChannelFeedback>,
}

impl ClientContext for MessengerProducerContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = false;
}
impl ProducerContext for MessengerProducerContext {
    type DeliveryOpaque = Box<MessengerProducerDeliveryOpaque>;

    fn delivery(&self, delivery_result: &rdkafka::producer::DeliveryResult<'_>, delivery_opaque: Self::DeliveryOpaque) {
        let result = delivery_result.as_ref();

        let version = delivery_opaque.version;

        match result {
            Ok(msg) => {
                info!("Message {:?} {:?}", msg.key(), msg.offset());
                // Safe to ignore error check, as error occurs only if receiver is closed or dropped, which would happen if the thread receving has errored. In such a scenario, the publisher thread would also shutdown.
                if let Err(error) = block_on(self.tx_feedback_channel.send(MessengerChannelFeedback::Success(version, "kafka".to_string()))) {
                    error!("[Messenger Producer Context] Error sending feedback for version={version} with error={error:?}");
                };
            }
            Err((publish_error, borrowed_message)) => {
                error!(
                    "[Messenger Producer Context] Error for version={:?} \nerror={:?}",
                    delivery_opaque.version,
                    publish_error.to_string()
                );
                let messenger_error = MessengerActionError {
                    kind: talos_messenger_core::errors::MessengerActionErrorKind::Publishing,
                    reason: publish_error.to_string(),
                    data: format!("version={version} message={:#?}", borrowed_message.detach()),
                };
                // Safe to ignore error check, as error occurs only if receiver is closed or dropped, which would happen if the thread receving has errored. In such a scenario, the publisher thread would also shutdown.
                if let Err(send_error) = block_on(self.tx_feedback_channel.send(MessengerChannelFeedback::Error(
                    version,
                    "kafka".to_string(),
                    Box::new(messenger_error),
                ))) {
                    error!("[Messenger Producer Context] Error sending error feedback for version={version} with \npublish_error={publish_error:?} \nchannel send_error={send_error:?}");
                };
            }
        }
    }
}
