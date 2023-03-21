use async_trait::async_trait;
use std::collections::HashMap;

use crate::errors::SystemServiceError;

use super::{common::SharedPortTraits, errors::MessageReceiverError};

// The trait that should be implemented by any adapter that will read the message
// and pass to the domain.
#[async_trait]
pub trait MessageReciever: SharedPortTraits {
    type Message;

    async fn consume_message(&mut self) -> Result<Option<Self::Message>, MessageReceiverError>;
    async fn subscribe(&self) -> Result<(), SystemServiceError>;
    async fn commit(&self, vers: u64) -> Result<(), SystemServiceError>;
    async fn unsubscribe(&self);
}

// The trait that should be implemented by any adapter that will publish the Decision message from Certifier Domain.
#[async_trait]
pub trait MessagePublisher: SharedPortTraits {
    async fn publish_message(&self, key: &str, value: &str, headers: Option<HashMap<String, String>>) -> Result<(), SystemServiceError>;
}
