use strum::Display;
use thiserror::Error as ThisError;

#[derive(Debug, Display, PartialEq)]
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

impl MessagingError {
    pub fn corrupted(reason: String, cause: Option<String>) -> Self {
        Self {
            kind: MessagingErrorKind::CorruptedPayload,
            reason,
            cause,
        }
    }
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

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_constructors() {
        let error = MessagingError::new_consuming("c1".to_string());
        assert_eq!(error.kind, MessagingErrorKind::Consuming);

        let error = MessagingError::new_corrupted_payload("r1".to_string(), "c1".to_string());
        assert_eq!(error.kind, MessagingErrorKind::CorruptedPayload);

        let error = MessagingError::new_publishing("r1".to_string(), "c1".to_string());
        assert_eq!(error.kind, MessagingErrorKind::Publishing);

        let error: MessagingError = "generic".to_string().into();
        assert_eq!(error.kind, MessagingErrorKind::Generic);
    }
}
// $coverage:ignore-end
