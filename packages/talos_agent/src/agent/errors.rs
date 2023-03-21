use crate::agent::errors::AgentErrorKind::{CertificationTimout, Messaging};
use crate::agent::model::CertifyRequestChannelMessage;
use crate::messaging::errors::MessagingError;
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
    pub cause: Option<String>,
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
            cause: None,
        }
    }
}

impl From<SendError<CertifyRequestChannelMessage>> for AgentError {
    fn from(e: SendError<CertifyRequestChannelMessage>) -> Self {
        AgentError {
            kind: AgentErrorKind::Certification {
                xid: e.0.request.candidate.xid,
            },
            reason: "Outgoing channel is closed".to_string(),
            cause: None,
        }
    }
}
