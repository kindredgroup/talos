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
    model::CandidateMessage,
    ports::{errors::MessageReceiverErrorKind, MessageReciever},
    ChannelMessage,
};

#[derive(Debug)]
pub struct MessageReceiverServiceConfig {
    /// Minimum offset lag to retain.
    /// Higher the value, lesser the frequency of commit.
    /// This would help in hydrating the suffix in certifier so as to help suffix retain a minimum size to
    /// reduce the initial aborts for fresh certification requests.
    pub min_commit_threshold: u64,
    /// Frequency at which to commit should be issued in ms
    pub commit_frequency_ms: u64,
}

pub struct MessageReceiverService {
    pub receiver: Box<dyn MessageReciever<Message = ChannelMessage<CandidateMessage>> + Send + Sync>,
    pub message_channel_tx: mpsc::Sender<ChannelMessage<CandidateMessage>>,
    pub commit_offset: Arc<AtomicI64>,
    pub system: System,
    pub commit_interval: Interval,
    pub config: MessageReceiverServiceConfig,
    /// The last offset that was committed.
    pub last_committed_offset: i64,
}

impl MessageReceiverService {
    pub fn new(
        receiver: Box<dyn MessageReciever<Message = ChannelMessage<CandidateMessage>> + Send + Sync>,
        message_channel_tx: mpsc::Sender<ChannelMessage<CandidateMessage>>,
        commit_offset: Arc<AtomicI64>,
        system: System,
        config: MessageReceiverServiceConfig,
    ) -> Self {
        Self {
            receiver,
            message_channel_tx,
            system,
            commit_offset,
            commit_interval: tokio::time::interval(Duration::from_millis(config.commit_frequency_ms)),
            config,
            last_committed_offset: 0,
        }
    }

    pub async fn subscribe(&self) -> Result<(), SystemServiceError> {
        self.receiver.subscribe().await?;
        Ok(())
    }

    /// Get the offset to commit after checking against the minimum lag to maintain.
    pub fn get_commit_offset(&self) -> Option<i64> {
        let offset = self.commit_offset.load(std::sync::atomic::Ordering::Relaxed);
        let offset_to_commit = offset - self.config.min_commit_threshold as i64;

        // If we cannot maintain the minimum offset, we return None.
        if offset_to_commit < 0 {
            None
        } else {
            Some(offset_to_commit)
        }
    }

    pub fn try_commit(&mut self) {
        if let Some(offset) = self.get_commit_offset() {
            if let Err(update_error) = self.receiver.update_offset_to_commit(offset) {
                error!("Failed to update offset {offset} with error {update_error:?}");
            } else if let Err(e) = self.receiver.commit() {
                error!("Failed to commit offset {offset} with error {e:?}");
            };
        }
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
            self.try_commit();
          }
        }

        Ok(())
    }
}
