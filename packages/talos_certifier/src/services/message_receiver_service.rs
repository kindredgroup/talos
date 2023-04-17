use std::{
    sync::{atomic::AtomicI64, Arc},
    time::Duration,
};

use async_trait::async_trait;
use log::{error, info, warn};
use tokio::sync::mpsc;

use crate::{
    core::{ServiceResult, System, SystemService},
    errors::{SystemErrorType, SystemServiceError, SystemServiceErrorKind},
    ports::{errors::MessageReceiverErrorKind, MessageReciever},
    ChannelMessage,
};

type PreviousCommitVers = u64;
type LatestCommitVers = u64;
pub struct MessageReceiverService {
    pub receiver: Box<dyn MessageReciever<Message = ChannelMessage> + Send + Sync>,
    pub message_channel_tx: mpsc::Sender<ChannelMessage>,
    pub commit_vers: (PreviousCommitVers, LatestCommitVers),
    pub commit_offset: Arc<AtomicI64>,
    pub system: System,
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
            commit_vers: (0, 0),
            commit_offset,
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
        let mut interval = tokio::time::interval(Duration::from_millis(10_000));
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
          _ = interval.tick() => {
            let offset = self.commit_offset.load(std::sync::atomic::Ordering::Relaxed);
            self.commit_vers.1 = offset.try_into().unwrap();

            let (prev_commit_vers, lastest_commit_vers) = self.commit_vers;
            if prev_commit_vers < lastest_commit_vers {
                self.receiver.commit(self.commit_vers.1).await?;
                self.commit_vers.0 = lastest_commit_vers;
            }
          }
        }

        Ok(())
    }
}
