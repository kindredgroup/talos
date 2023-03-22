use crate::agent::errors::AgentErrorKind::{CertificationTimout, Messaging};
use crate::agent::model::CertifyRequestChannelMessage;
use crate::messaging::errors::MessagingError;
use strum::Display;
use thiserror::Error as ThisError;
use tokio::sync::mpsc::error::SendError;

#[derive(Debug, Display, PartialEq)]
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

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{CandidateData, CertificationRequest, CertificationResponse};
    use crate::messaging::errors::MessagingErrorKind;
    use crate::mpsc::core::Sender;
    use async_trait::async_trait;
    use log::LevelFilter;
    use mockall::mock;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc::error::SendError;

    mock! {
        NoopSender {}

        #[async_trait]
        impl Sender for NoopSender {
            type Data = CertificationResponse;
            pub async fn send(&self, value: CertificationResponse) -> Result<(), SendError<CertificationResponse>> {}
        }
    }

    #[test]
    fn new_certify_timout() {
        let _ = env_logger::builder().filter_level(LevelFilter::Debug).format_timestamp_millis().try_init();

        let agent_error = AgentError::new_certify_timout("xid".to_string(), 111);
        assert_eq!(
            agent_error.kind,
            CertificationTimout {
                xid: "xid".to_string(),
                elapsed_ms: 111,
            }
        );
        let _ = format!("Debug coverage: {:?} {}", agent_error, agent_error);
    }

    #[test]
    fn convert_from_send_error() {
        let _ = env_logger::builder().filter_level(LevelFilter::Debug).format_timestamp_millis().try_init();

        let send_error = SendError(CertifyRequestChannelMessage::new(
            &CertificationRequest {
                message_key: "key1".to_string(),
                candidate: CandidateData {
                    xid: "xid".to_string(),
                    readset: vec![String::from("1"), String::from("2"), String::from("3")],
                    readvers: vec![1_u64, 2_u64, 3_u64],
                    snapshot: 0,
                    writeset: vec![String::from("1"), String::from("2"), String::from("3")],
                },
                timeout: Some(Duration::from_secs(1)),
            },
            Arc::new(Box::new(MockNoopSender::new())),
        ));

        let agent_error: AgentError = send_error.into();
        assert_eq!(agent_error.kind, AgentErrorKind::Certification { xid: "xid".to_string() });
        let _ = format!("Debug coverage: {:?} {}", agent_error, agent_error);
    }

    #[test]
    fn convert_from_messaging_error() {
        let _ = env_logger::builder().filter_level(LevelFilter::Debug).format_timestamp_millis().try_init();

        let messaging_error = MessagingError {
            kind: MessagingErrorKind::Generic,
            reason: "some reason...".to_string(),
            cause: None,
        };

        let agent_error: AgentError = messaging_error.into();
        assert_eq!(agent_error.kind, Messaging);
        let _ = format!("Debug coverage: {:?} {}", agent_error, agent_error);
    }
}
// $coverage:ignore-end
