use ahash::HashMap;
use async_trait::async_trait;
use tokio::task::JoinHandle;

use crate::errors::SystemServiceError;

use super::{common::SharedPortTraits, errors::MessageReceiverError};

// The trait that should be implemented by any adapter that will read the message
// and pass to the domain.
#[async_trait]
pub trait MessageReciever: SharedPortTraits {
    type Message;

    async fn consume_message(&mut self) -> Result<Option<Self::Message>, MessageReceiverError>;
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
