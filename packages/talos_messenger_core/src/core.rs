use ahash::HashMap;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use strum::{Display, EnumIter, EnumString};
use talos_certifier::ports::errors::MessagePublishError;

use crate::errors::{MessengerActionError, MessengerServiceResult};

#[derive(Debug, Display, Serialize, Deserialize, EnumString, EnumIter, Clone, Eq, PartialEq)]
pub enum CommitActionType {
    #[strum(serialize = "publish")]
    Publish,
}

#[derive(Debug, Display, Serialize, Deserialize, EnumString, Clone, Eq, PartialEq)]
pub enum PublishActionType {
    #[strum(serialize = "kafka")]
    Kafka,
}

#[async_trait]
pub trait MessengerPublisher {
    type Payload;
    type AdditionalData;
    fn get_publish_type(&self) -> PublishActionType;
    async fn send(
        &self,
        version: u64,
        payload: Self::Payload,
        headers: HashMap<String, String>,
        additional_data: Self::AdditionalData,
    ) -> Result<(), MessagePublishError>;
}

/// Trait for any action service.
#[async_trait]
pub trait ActionService {
    async fn process_action(&mut self) -> MessengerServiceResult;
}

/// Trait to be implemented by all services.
#[async_trait]
pub trait MessengerSystemService {
    async fn start(&self) -> MessengerServiceResult;
    async fn run(&mut self) -> MessengerServiceResult;
    async fn stop(&self) -> MessengerServiceResult;
}

#[derive(Debug, Clone)]
pub struct MessengerCommitActions {
    pub version: u64,
    pub commit_actions: HashMap<String, Value>,
    pub headers: HashMap<String, String>,
}

pub enum MessengerChannelFeedback {
    Error(u64, String, Box<MessengerActionError>),
    Success(u64, String),
}
