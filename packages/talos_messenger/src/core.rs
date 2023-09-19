use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use strum::{Display, EnumIter, EnumString};

use crate::errors::MessengerServiceResult;

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
    fn get_publish_type(&self) -> PublishActionType;
    async fn send(&self, payload: Self::Payload) -> ();
}

/// Trait to be implemented by all services.
#[async_trait]
pub trait MessengerSystemService {
    async fn start(&self) -> MessengerServiceResult;
    async fn run(&mut self) -> MessengerServiceResult;
    async fn stop(&self) -> MessengerServiceResult;
}

#[derive(Debug)]
pub struct MessengerCommitActions {
    pub version: u64,
    pub commit_actions: Box<Value>,
}

pub enum MessengerChannelFeedback {
    Error(u64, String),
    Success(u64, u32),
}
