use std::{
    sync::{atomic::AtomicI64, Arc},
    time::Duration,
};

use async_trait::async_trait;
use log::{error, info, warn};
use tokio::{sync::mpsc, time::Interval};

use crate::{
    core::{ServiceResult, System, SystemService},
    errors::{SystemErrorType, SystemServiceError, SystemServiceErrorKind},
    ports::{errors::MessageReceiverErrorKind, MessageReciever},
    ChannelMessage,
};

pub struct MessageReceiverService {
    pub receiver: Box<dyn MessageReciever<Message = ChannelMessage> + Send + Sync>,
    pub message_channel_tx: mpsc::Sender<ChannelMessage>,
    pub commit_offset: Arc<AtomicI64>,
    pub system: System,
    pub commit_interval: Interval,
}

impl MessageReceiverService {
    pub fn new(
        receiver: Box<dyn MessageReciever<Message = ChannelMessage> + Send + Sync>,
        message_channel_tx: mpsc::Sender<ChannelMessage>,
        commit_offset: Arc<AtomicI64>,
        system: System,
    ) -> Self {
        Self {
            receiver,
            message_channel_tx,
            system,
            commit_offset,
            commit_interval: tokio::time::interval(Duration::from_millis(10_000)),
        }
    }

    pub async fn subscribe(&self) -> Result<(), SystemServiceError> {
        self.receiver.subscribe().await?;
        Ok(())
    }
}

#[async_trait]
impl SystemService for MessageReceiverService {
    async fn run(&mut self) -> ServiceResult {
        tokio::select! {
          // ** Consume Messages from Kafka
          res = self.receiver.consume_message() => {
            match res {
                Ok(Some(msg))  => {

                    if let Err(error) = self.message_channel_tx.send(msg.clone()).await {
                        return Err(Box::new(SystemServiceError{
                             kind: SystemServiceErrorKind::SystemError(SystemErrorType::Channel),
                             reason: error.to_string(),
                             data: Some(format!("{:?}", msg)),
                             service: "Message Receiver Service".to_string()
                             }))
                    }
                },
                Ok(None) => {
                    info!("Consume message returned None");
                }
                Err(consumer_error) => {
                    match &consumer_error.kind {
                        MessageReceiverErrorKind::SubscribeError => {
                            error!("{:?} ", consumer_error.to_string());
                            // *** Shutdown the current service and return the error
                            // self.shutdown_service().await;
                            return Err(Box::new(consumer_error.into()))
                        },
                        // These should log and but not stop the system.
                        _ => {
                            warn!("{:?} ", consumer_error.to_string());
                        }
                    }
                },
            }
          }
          //** commit message
          _ = self.commit_interval.tick() => {
            let offset = self.commit_offset.load(std::sync::atomic::Ordering::Relaxed);
            self.receiver.update_savepoint(offset)?;
            self.receiver.commit_async();
          }
        }

        Ok(())
    }
}
