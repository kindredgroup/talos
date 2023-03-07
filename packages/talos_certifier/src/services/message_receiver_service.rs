use std::{
    sync::{atomic::AtomicI64, Arc},
    time::Duration,
};

use async_trait::async_trait;
use log::{debug, error, info};
use tokio::sync::mpsc;

use crate::{
    core::{System, SystemService},
    errors::{SystemErrorType, SystemServiceError, SystemServiceErrorKind},
    ports::MessageReciever,
    ChannelMessage, SystemMessage,
};

type PreviousCommitVers = u64;
type LatestCommitVers = u64;
pub struct MessageReceiverService {
    pub receiver: Box<dyn MessageReciever<Message = ChannelMessage> + Send + Sync>,
    pub message_channel_tx: mpsc::Sender<ChannelMessage>,
    pub decision_message_channel_tx: mpsc::Sender<ChannelMessage>,
    pub commit_vers: (PreviousCommitVers, LatestCommitVers),
    pub commit_offset: Arc<AtomicI64>,
    pub system: System,
}

impl MessageReceiverService {
    pub fn new(
        receiver: Box<dyn MessageReciever<Message = ChannelMessage> + Send + Sync>,
        message_channel_tx: mpsc::Sender<ChannelMessage>,
        decision_message_channel_tx: mpsc::Sender<ChannelMessage>,
        commit_offset: Arc<AtomicI64>,
        system: System,
    ) -> Self {
        Self {
            receiver,
            message_channel_tx,
            decision_message_channel_tx,
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
    //** Initiate Shutdown

    async fn shutdown_service(&mut self) {
        debug!("Recevier Service shutting down");
        self.receiver.shutdown().await;
        self.system.is_shutdown = true;
        info!("Receiver Service shutdown completed!");
    }

    fn is_shutdown(&self) -> bool {
        self.system.is_shutdown
    }

    async fn update_shutdown_flag(&mut self, flag: bool) {
        self.system.is_shutdown = flag;
    }
    async fn health_check(&self) -> bool {
        self.receiver.is_healthy().await
    }

    async fn run(&mut self) -> Result<(), SystemServiceError> {
        let mut system_channel_rx = self.system.system_notifier.subscribe();
        let mut interval = tokio::time::interval(Duration::from_millis(10_000));
        // while !self.is_shutdown() {
        tokio::select! {
          // ** Consume Messages from Kafka
          res = self.receiver.consume_message() => {
            match res {
                Ok(Some(msg))  => {
                    match &msg {
                        ChannelMessage::Candidate(_) => {
                            if let Err(error) = self.message_channel_tx.send(msg.clone()).await {
                                return Err(SystemServiceError{
                                     kind: SystemServiceErrorKind::SystemError(SystemErrorType::Channel),
                                     reason: error.to_string(),
                                     data: Some(format!("{:?}", msg)),
                                     service: "Message Receiver Service".to_string()
                                     })
                            };
                        },
                        ChannelMessage::Decision(..) => {
                            if let Err(error) = self.decision_message_channel_tx.send(msg.clone()).await {
                                return Err(SystemServiceError{
                                     kind: SystemServiceErrorKind::SystemError(SystemErrorType::Channel),
                                     reason: error.to_string(),
                                     data: Some(format!("{:?}", msg)),
                                     service: "Message Receiver Service".to_string()
                                     })
                            };
                        },
                    }
                },
                Ok(None) => {
                    info!("Consume message returned None");
                }
                Err(consumer_error) => {
                    match &consumer_error.kind {
                        SystemServiceErrorKind::ParseError => {
                            error!("Parse error {:?} ",  consumer_error);
                            // self.shutdown_service().await;
                            // return Err(consumer_error)
                        },
                        _ => {
                                // *** Shutdown the current service and return the error
                                self.shutdown_service().await;
                                return Err(consumer_error)
                            }
                        // talos_core::errors::SystemServiceErrorKind::DBError => todo!(),
                        // talos_core::errors::SystemServiceErrorKind::CertifierError => todo!(),
                        // talos_core::errors::SystemServiceErrorKind::SystemError => todo!(),
                        // talos_core::errors::SystemServiceErrorKind::MessageReceiverError(_) => todo!(),
                        // talos_core::errors::SystemServiceErrorKind::MessagePublishError => todo!(),
                        // talos_core::errors::SystemServiceErrorKind::SomeError => todo!(),
                    }
                },
            }
          }
          //** commit message
          _ = interval.tick() => {
        //   _ = tokio::time::sleep(Duration::from_millis(5_000)) => {
            // error!("Previous version {prev_commit_vers} & Current Committed version {lastest_commit_vers}");
            let offset = self.commit_offset.load(std::sync::atomic::Ordering::Relaxed);
            self.commit_vers.1 = offset.try_into().unwrap();

            let (prev_commit_vers, lastest_commit_vers) = self.commit_vers;
            if prev_commit_vers < lastest_commit_vers {
                self.receiver.commit(self.commit_vers.1).await?;
                self.commit_vers.0 = lastest_commit_vers;
            }
          }
          // ** Received System Messages (shutdown/healthcheck).
          msg = system_channel_rx.recv() => {
            let message = msg.unwrap();

            match message {
              SystemMessage::Shutdown => {
                info!("[Message Receiver Service] Shutdown received");
                self.shutdown_service().await;
              },
              SystemMessage::HealthCheck => {
                // info!("Health Check message received <3 <3 <3");
                let is_healthy = self.health_check().await;
                self.system.system_notifier.send(SystemMessage::HealthCheckStatus { service: "KAFKA_CONSUMER", healthy: is_healthy },).unwrap();
              },
              _ => ()
           }

          }
        }
        // }

        //TODO: any final clean ups before exiting
        // debug!("Exiting MessageReceiverService::run");

        Ok(())
    }
}
