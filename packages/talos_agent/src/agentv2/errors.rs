use crate::agentv2::errors::AgentErrorKind::{CertificationTimout, Messaging};
use crate::agentv2::model::CertifyRequestChannelMessage;
use crate::messaging::errors::MessagingError;
use std::error::Error;
use strum::Display;
use thiserror::Error as ThisError;
use tokio::sync::mpsc::error::SendError;

#[derive(Debug, Display)]
pub enum AgentErrorKind {
    Certification { xid: String },
    CertificationTimout { xid: String, elapsed_ms: u128 },
    Messaging,
    Internal,
}

#[derive(Debug, ThisError)]
#[error("Talos agent error: '{kind}'.\nReason: {reason}\nCause: {cause:?}")]
pub struct AgentError {
    pub kind: AgentErrorKind,
    pub reason: String,
    pub cause: Option<Box<dyn Error + Send>>,
}

impl AgentError {
    pub fn new_certify_timout(xid: String, elapsed_ms: u128) -> AgentError {
        AgentError {
            kind: CertificationTimout { xid, elapsed_ms },
            reason: "Timeout".to_string(),
            cause: None,
        }
    }
}

impl From<MessagingError> for AgentError {
    fn from(e: MessagingError) -> Self {
        AgentError {
            kind: Messaging,
            reason: e.to_string(),
            cause: e.cause,
        }
    }
}

impl From<SendError<CertifyRequestChannelMessage>> for AgentError {
    fn from(e: SendError<CertifyRequestChannelMessage>) -> Self {
        AgentError {
            kind: AgentErrorKind::Certification {
                xid: e.0.request.candidate.xid.clone(),
            },
            reason: "Outgoing channel is closed".to_string(),
            cause: Some(Box::new(e)),
        }
    }
}
