use crate::agentv2::errors::AgentStartError::MessagingConnectivity;
use crate::messaging::errors::MessagingError;
use thiserror::Error as ThisError;

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
