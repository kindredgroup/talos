use std::time::Duration;

use ahash::HashMap;
use async_trait::async_trait;
use tokio::task::JoinHandle;

use crate::errors::SystemServiceError;

use super::{common::SharedPortTraits, errors::MessageReceiverError};

#[derive(Debug, Clone)]
pub enum ConsumeMessageTimeoutType {
    /// No timeout to be applied.
    NoTimeout,
    /// General timeout. With timeout in mx
    Timeout(u64),
    /// When we are constantly at max. With the number of iterations it has been at max.
    SteadyAtMax(u64),
}

// The trait that should be implemented by any adapter that will read the message
// and pass to the domain.
#[async_trait]
pub trait MessageReciever: SharedPortTraits {
    type Message;

    async fn consume_message(&mut self) -> Result<Option<Self::Message>, MessageReceiverError>;
    async fn consume_message_with_timeout(&mut self, timeout: ConsumeMessageTimeoutType) -> Result<Option<Self::Message>, MessageReceiverError> {
        match timeout {
            ConsumeMessageTimeoutType::NoTimeout => unimplemented!(),
            ConsumeMessageTimeoutType::Timeout(time_ms) => {
                tokio::time::sleep(Duration::from_millis(time_ms)).await;
            }
            ConsumeMessageTimeoutType::SteadyAtMax(_) => unimplemented!(),
        }

        self.consume_message().await
    }
    async fn subscribe(&self) -> Result<(), SystemServiceError>;
    fn commit(&self) -> Result<(), Box<SystemServiceError>>;
    fn commit_async(&self) -> Option<JoinHandle<Result<(), SystemServiceError>>>;
    fn update_offset_to_commit(&mut self, offset: i64) -> Result<(), Box<SystemServiceError>>;
    async fn update_savepoint_async(&mut self, offset: i64) -> Result<(), SystemServiceError>;
    async fn unsubscribe(&self);
}

// The trait that should be implemented by any adapter that will publish the Decision message from Certifier Domain.
#[async_trait]
pub trait MessagePublisher: SharedPortTraits {
    async fn publish_message(&self, key: &str, value: &str, headers: HashMap<String, String>) -> Result<(), SystemServiceError>;
}
