use ahash::HashMap;
use async_trait::async_trait;
use strum::{Display, EnumString};
use tokio::sync::broadcast;

use crate::{
    errors::SystemServiceError,
    model::{CandidateMessage, DecisionMessage},
};

#[derive(Debug, Clone)]
pub struct CandidateChannelMessage {
    pub message: CandidateMessage,
    pub headers: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct DecisionChannelMessage {
    pub decision_version: u64,
    pub message: DecisionMessage,
    pub headers: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub enum ChannelMessage {
    Candidate(Box<CandidateChannelMessage>),
    Decision(Box<DecisionChannelMessage>),
}

#[derive(Debug, Display, Eq, PartialEq, EnumString)]
pub enum MessageVariant {
    Candidate,
    Decision,
}

#[derive(Debug, Clone, PartialEq)]

pub enum SystemMessage {
    Shutdown,
    ShutdownWithError(Box<SystemServiceError>),
    HealthCheck,
    HealthCheckStatus { service: &'static str, healthy: bool },
}

pub type ServiceResult<T = ()> = Result<T, Box<SystemServiceError>>;

#[derive(Debug)]
pub struct DecisionOutboxChannelMessage {
    pub message: DecisionMessage,
    pub headers: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct System {
    pub system_notifier: broadcast::Sender<SystemMessage>,
    pub is_shutdown: bool,
    /// Unique identifier of the system - container or pod name/id
    pub name: String,
}

#[async_trait]
pub trait SystemService {
    async fn run(&mut self) -> ServiceResult;
}
