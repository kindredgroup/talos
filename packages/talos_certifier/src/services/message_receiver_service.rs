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
    services::MetricsServiceMessage,
    ChannelMessage,
};

pub struct MessageReceiverService {
    pub receiver: Box<dyn MessageReciever<Message = ChannelMessage> + Send + Sync>,
    pub message_channel_tx: mpsc::Sender<ChannelMessage>,
    pub commit_offset: Arc<AtomicI64>,
    pub system: System,
    pub commit_interval: Interval,
    pub metrics_tx: mpsc::Sender<MetricsServiceMessage>,
}

impl MessageReceiverService {
    pub fn new(
        receiver: Box<dyn MessageReciever<Message = ChannelMessage> + Send + Sync>,
        message_channel_tx: mpsc::Sender<ChannelMessage>,
        commit_offset: Arc<AtomicI64>,
        system: System,
        metrics_tx: mpsc::Sender<MetricsServiceMessage>,
    ) -> Self {
        Self {
            receiver,
            message_channel_tx,
            system,
            commit_offset,
            commit_interval: tokio::time::interval(Duration::from_millis(10_000)),
            metrics_tx,
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
        // error!("inside Message service");
        tokio::select! {
          // ** Consume Messages from Kafka
          res = self.receiver.consume_message() => {
            match res {
                Ok(Some(msg))  => {


                    // if self.message_channel_tx.capacity() + 100 >= self.message_channel_tx.max_capacity() {
                    //     error!(
                    //         "[message_channel] Almost near the max capacity. Capacity={} and max_capacity={} ",
                    //         self.message_channel_tx.capacity(),
                    //         self.message_channel_tx.max_capacity()
                    //     )
                    // }

                    if let Err(error) = self.message_channel_tx.send(msg.clone()).await {
                        return Err(Box::new(SystemServiceError{
                             kind: SystemServiceErrorKind::SystemError(SystemErrorType::Channel),
                             reason: error.to_string(),
                             data: Some(format!("{:?}", msg)),
                             service: "Message Receiver Service".to_string()
                             }))
                    }

                    let capacity_percentage = self.message_channel_tx.capacity() / self.message_channel_tx.max_capacity() * 100;

                    let metrics_tx_cloned = self.metrics_tx.clone();

                    tokio::spawn(async move {
                        let _ = metrics_tx_cloned
                            .send(MetricsServiceMessage::Record(
                                "CHANNEL - M2C capacity (%)".to_string(),
                                capacity_percentage as u64,
                            ))
                            .await;
                    });
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
            self.receiver.update_offset_to_commit(offset)?;
            self.receiver.commit_async();
          }
        }

        Ok(())
    }
}
