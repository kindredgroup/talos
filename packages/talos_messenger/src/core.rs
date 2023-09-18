use async_trait::async_trait;
use serde_json::Value;

use crate::{errors::MessengerServiceResult, models::commit_actions::publish::OnCommitActions};

#[async_trait]
pub trait MessengerPublisher {
    async fn send(&self) -> ();
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
