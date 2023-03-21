use strum::Display;
use thiserror::Error as ThisError;

#[derive(Debug, Display)]
pub enum MessagingErrorKind {
    Consuming,
    CorruptedPayload,
    Generic,
    Publishing,
}

#[derive(Debug, ThisError)]
#[error("'{kind}' error with messaging system.\nReason: {reason}\nCause: {cause:?}")]
pub struct MessagingError {
    pub kind: MessagingErrorKind,
    pub reason: String,
    pub cause: Option<String>,
}

impl From<String> for MessagingError {
    fn from(reason: String) -> Self {
        MessagingError {
            kind: MessagingErrorKind::Generic,
            reason,
            cause: None,
        }
    }
}

impl MessagingError {
    pub fn new_consuming(cause: String) -> Self {
        MessagingError {
            kind: MessagingErrorKind::Consuming,
            reason: "Cannot read message".to_string(),
            cause: Some(cause),
        }
    }

    pub fn new_publishing(reason: String, cause: String) -> Self {
        MessagingError {
            kind: MessagingErrorKind::Publishing,
            reason,
            cause: Some(cause),
        }
    }

    pub fn new_corrupted_payload(reason: String, cause: String) -> Self {
        MessagingError {
            kind: MessagingErrorKind::CorruptedPayload,
            reason,
            cause: Some(cause),
        }
    }
}
