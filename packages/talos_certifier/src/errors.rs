use talos_suffix::errors::SuffixError;
use thiserror::Error as ThisError;

use crate::ports::errors::{DecisionStoreError, DecisionStoreErrorKind, MessagePublishError, MessageReceiverError, MessageReceiverErrorKind};

#[derive(Debug, ThisError)]

pub enum CertificationError {
    #[error(transparent)]
    SuffixError(#[from] SuffixError),
    #[error("Validation failed with reason={reason}. \n data={data:?}  ")]
    Validation {
        // code: String,
        reason: String,
        data: Option<String>,
    },
}

#[derive(Debug, Clone, ThisError, Eq, PartialEq)]
pub enum DecisionOutBoxServiceError {
    #[error("| OUT_BOUND ERROR | XDB Error ")]
    XDBError(#[source] DecisionStoreError),
    #[error("| OUT_BOUND ERROR | Publisher Error ")]
    PublishError(#[source] MessagePublishError),
}

#[derive(Debug, ThisError)]
pub enum CommonError {
    #[error("Parse Error input={data} with reason={reason}")]
    ParseError { data: String, reason: String },
}

#[derive(Debug, PartialEq, Clone)]
pub struct AdapterFailureError {
    pub adapter_name: String,
    pub reason: String,
}

#[derive(Debug, PartialEq, Clone)]
pub enum SystemErrorType {
    Channel,
    HealthCheck,
    Generic,
    AdapterFailure(AdapterFailureError),
}

#[derive(Debug, PartialEq, Clone)]
pub enum SystemServiceErrorKind {
    ParseError,
    DBError,
    CertifierError,
    SystemError(SystemErrorType),
    MessageReceiverError(MessageReceiverErrorKind),
    MessagePublishError,
}

#[derive(Debug, ThisError, Clone)]
#[error("error on service={service} kind={kind:?} \n reason={reason} \n data={data:?}")]
pub struct SystemServiceError {
    pub kind: SystemServiceErrorKind,
    pub reason: String,
    pub data: Option<String>,
    pub service: String,
}

impl From<CommonError> for SystemServiceError {
    fn from(inner: CommonError) -> Self {
        match inner {
            CommonError::ParseError { data, reason } => SystemServiceError {
                kind: SystemServiceErrorKind::ParseError,
                reason,
                data: Some(data),
                service: "Service Info to be mapped".to_string(),
            },
        }
    }
}

impl From<DecisionOutBoxServiceError> for Box<SystemServiceError> {
    fn from(inner: DecisionOutBoxServiceError) -> Self {
        match inner {
            DecisionOutBoxServiceError::XDBError(dberror) => {
                if dberror.kind == DecisionStoreErrorKind::ParseError {
                    Box::new(SystemServiceError {
                        kind: SystemServiceErrorKind::ParseError,
                        reason: dberror.reason,
                        data: dberror.data,
                        service: "DecisionOutBoxService".to_string(),
                    })
                } else {
                    Box::new(SystemServiceError {
                        kind: SystemServiceErrorKind::DBError,
                        reason: dberror.reason,
                        data: dberror.data,
                        service: "DecisionOutBoxService".to_string(),
                    })
                }
            }
            DecisionOutBoxServiceError::PublishError(pub_error) => Box::new(SystemServiceError {
                kind: SystemServiceErrorKind::MessagePublishError,
                reason: pub_error.reason,
                data: pub_error.data,
                service: "DecisionOutBoxService".to_string(),
            }),
        }
    }
}

impl From<CertificationError> for Box<SystemServiceError> {
    fn from(certifier_error: CertificationError) -> Self {
        match &certifier_error {
            CertificationError::SuffixError(SuffixError::VersionToIndexConversionError(v)) => Box::new(SystemServiceError {
                kind: SystemServiceErrorKind::CertifierError,
                reason: certifier_error.to_string(),
                data: Some(v.to_string()),
                service: "CertifierService".to_string(),
            }),
            CertificationError::SuffixError(_) => Box::new(SystemServiceError {
                kind: SystemServiceErrorKind::CertifierError,
                reason: certifier_error.to_string(),
                data: None,
                service: "CertifierService".to_string(),
            }),
            CertificationError::Validation { .. } => Box::new(SystemServiceError {
                kind: SystemServiceErrorKind::CertifierError,
                reason: certifier_error.to_string(),
                data: None,
                service: "CertifierService".to_string(),
            }),
        }
    }
}
impl From<MessageReceiverError> for SystemServiceError {
    fn from(msg_rx_error: MessageReceiverError) -> Self {
        match msg_rx_error.kind {
            MessageReceiverErrorKind::ParseError => SystemServiceError {
                kind: SystemServiceErrorKind::ParseError,
                reason: msg_rx_error.reason,
                data: msg_rx_error.data,
                service: "Message Receiver Servicer".to_string(),
            },
            _ => SystemServiceError {
                kind: SystemServiceErrorKind::MessageReceiverError(msg_rx_error.kind),
                reason: msg_rx_error.reason,
                data: msg_rx_error.data,
                service: "Message Receiver Servicer".to_string(),
            },
            // MessageReceiverErrorKind::IncorrectData => todo!(),
            // MessageReceiverErrorKind::VersionZero => todo!(),
            // MessageReceiverErrorKind::SaveVersion => todo!(),
            // MessageReceiverErrorKind::SubscribeError => todo!(),
            // MessageReceiverErrorKind::CommitError => todo!(),
        }
    }
}
impl From<MessagePublishError> for SystemServiceError {
    fn from(msg_rx_error: MessagePublishError) -> Self {
        SystemServiceError {
            kind: SystemServiceErrorKind::MessagePublishError,
            reason: msg_rx_error.reason,
            data: msg_rx_error.data,
            service: "Message Publish Service".to_string(),
        }
    }
}
