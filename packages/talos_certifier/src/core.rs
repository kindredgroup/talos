use async_trait::async_trait;
use strum::{Display, EnumString};
use tokio::sync::broadcast;

use crate::{
    errors::SystemServiceError,
    model::{CandidateMessage, DecisionMessage},
};

type Version = u64;
#[derive(Debug, Clone)]
pub enum ChannelMessage {
    Candidate(CandidateMessage),
    Decision(Version, DecisionMessage),
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
pub enum DecisionOutboxChannelMessage {
    Decision(DecisionMessage),
}

#[derive(Debug, Clone)]
pub struct System {
    pub system_notifier: broadcast::Sender<SystemMessage>,
    pub is_shutdown: bool,
}

#[async_trait]
pub trait SystemService {
    async fn run(&mut self) -> ServiceResult;
}
