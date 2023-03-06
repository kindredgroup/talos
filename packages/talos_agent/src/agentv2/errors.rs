use crate::agentv2::errors::AgentStartError::MessagingConnectivity;
use crate::agentv2::errors::CertifyError::InternalError;
use crate::agentv2::model::CertifyRequestChannelMessage;
use crate::messaging::errors::MessagingError;
use thiserror::Error as ThisError;
use tokio::sync::mpsc::error::SendError;

#[derive(Debug, ThisError)]
pub enum AgentStartError {
    /// Error connecting with messaging middleware
    #[error("Cannot connect to broker.\nReason: {reason}")]
    MessagingConnectivity { reason: String },
}

impl From<MessagingError> for AgentStartError {
    fn from(e: MessagingError) -> Self {
        MessagingConnectivity {
            reason: format!("Messaging system connectivity error.\nReason: {}{}", e.reason, e),
        }
    }
}

#[derive(Debug, ThisError)]
pub enum CertifyError {
    #[error("Certification error for XID {xid}.\nReason: {reason}")]
    InternalError { xid: String, reason: String },

    #[error("Certification request for XID {xid} timed out after {elapsed_ms}ms")]
    Timeout { xid: String, elapsed_ms: u128 },
}

impl From<SendError<CertifyRequestChannelMessage>> for CertifyError {
    fn from(e: SendError<CertifyRequestChannelMessage>) -> Self {
        InternalError {
            xid: e.0.request.candidate.xid,
            reason: "Outgoing channel is closed".to_string(),
        }
    }
}
