use ahash::HashMap;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use strum::{Display, EnumString};
use talos_common_utils::ResetVariantTrait;
use tokio::sync::broadcast;

use crate::{errors::SystemServiceError, model::DecisionMessage};

pub trait CandidateMessageBaseTrait: DeserializeOwned {
    fn get_xid(&self) -> &str;
    fn get_snapshot(&self) -> u64;
    fn get_version(&self) -> u64;
    fn get_agent(&self) -> &str;
    fn get_cohort(&self) -> &str;
    fn add_version(&mut self, version: u64);
    fn add_candidate_received_metric(&mut self, received_at: i128);
}

#[derive(Debug, Clone)]
pub struct CandidateChannelMessage<T: CandidateMessageBaseTrait> {
    pub message: T,
    pub headers: HashMap<String, String>,
}

impl<T: CandidateMessageBaseTrait> CandidateChannelMessage<T> {
    pub fn get_trace_parent(&self) -> Option<String> {
        self.headers.get("traceparent").cloned()
    }
}

#[derive(Debug, Clone)]
pub struct DecisionChannelMessage {
    pub decision_version: u64,
    pub message: DecisionMessage,
    pub headers: HashMap<String, String>,
}

impl DecisionChannelMessage {
    pub fn get_trace_parent(&self) -> Option<String> {
        self.headers.get("traceparent").cloned()
    }
}

#[derive(Debug, Clone)]
pub enum ChannelMessage<T: CandidateMessageBaseTrait> {
    Candidate(Box<CandidateChannelMessage<T>>),
    Decision(Box<DecisionChannelMessage>),
    Reset,
}

impl<T> ResetVariantTrait for ChannelMessage<T>
where
    T: CandidateMessageBaseTrait,
{
    const RESET_VARIANT: Self = ChannelMessage::Reset;
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
