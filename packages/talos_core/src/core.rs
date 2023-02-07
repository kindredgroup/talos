use strum::{Display, EnumString};

use crate::model::{candidate_message::CandidateMessage, decision_message::DecisionMessage};

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
