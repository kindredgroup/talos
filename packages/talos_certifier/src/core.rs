use async_trait::async_trait;
use strum::{Display, EnumString};
use tokio::sync::{broadcast, mpsc};

use crate::{
    errors::{SystemErrorType, SystemServiceError},
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

#[derive(Debug, Clone, Eq, PartialEq)]

pub enum SystemMessage {
    Shutdown,
    SaveState(u64),
    HealthCheck,
    HealthCheckStatus { service: &'static str, healthy: bool },
}

pub type ServiceResult<T = ()> = Result<T, Box<SystemServiceError>>;

#[derive(Debug)]
pub enum DecisionOutboxChannelMessage {
    Decision(DecisionMessage),
    OutboundServiceError(SystemServiceError),
}

#[derive(Debug)]
pub enum SystemMonitorMessage {
    Failures(SystemErrorType),
}

#[derive(Debug, Clone)]
pub struct System {
    pub system_notifier: broadcast::Sender<SystemMessage>,
    pub monitor_tx: mpsc::Sender<SystemMonitorMessage>,
    pub is_shutdown: bool,
}

#[async_trait]
pub trait SystemService {
    async fn shutdown_service(&mut self);
    fn is_shutdown(&self) -> bool;
    async fn update_shutdown_flag(&mut self, flag: bool);
    async fn health_check(&self) -> bool;
    async fn run(&mut self) -> ServiceResult;
}
