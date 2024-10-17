use ahash::HashMap;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use strum::{Display, EnumIter, EnumString};

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
    async fn send(&self, version: u64, payload: Self::Payload, headers: HashMap<String, String>, additional_data: Self::AdditionalData) -> ();
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

pub type TotalActionsForVersion = u32;

pub enum MessengerChannelFeedback {
    Error(u64, String, Box<MessengerActionError>, TotalActionsForVersion),
    Success(u64, String, TotalActionsForVersion),
}
