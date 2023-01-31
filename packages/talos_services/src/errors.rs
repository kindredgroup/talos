// use std::fmt::Debug;

// use suffix::errors::SuffixError;
// use talos_adapters::{KafkaAdapterError, PgError};
// use talos_core::ChannelMessage;
// use talos_ports::errors::{DecisionStoreErrorKind, MessageReceiverErrorKind};
// use thiserror::Error as ThisError;

// use tokio::sync::{broadcast, mpsc};

// use crate::{
//     core::SystemMessage,
//     services::{
//         certifier::CertifierServiceError, decision_outbox::DecisionOutBoxServiceError, health_check::HealthCheckServiceError,
//         message_receiver::MessageReceiverServiceError,
//     },
// };

// // Different possible errors
// // System Errors
// //  - Channel Error
// //  - Health Check Error
// // Domain Error
// //  - Certification Error
// // Infrastructure/Adapters Error
// //  - MessagingInboundError
// //  - MessagingOutboundError
// //  - DBError

// #[derive(Debug, ThisError)]
// pub enum SystemError {
//     // #[error("| SYSTEM_ERROR | Health Check Error: {0}")]
//     // HealthCheck(String),
//     #[error("| SYSTEM_ERROR | Error sending message over system broadcast channel ")]
//     SendSystemMessage(#[source] broadcast::error::SendError<SystemMessage>),
//     #[error("| SYSTEM_ERROR | Error sending message over channel ")]
//     ServiceChannelSendMessage(#[source] Box<mpsc::error::SendError<ChannelMessage>>),
// }

// #[derive(Debug, ThisError)]
// pub enum TalosErrorNew {}

// #[derive(Debug, ThisError)]
// pub enum SystemServiceErrorKind {
//     #[error("Error Parsing data")]
//     ParseError,
//     #[error("Database Error")]
//     DBError,
//     #[error("Certifier Error ")]
//     CertifierError,
//     #[error(transparent)]
//     SystemError(SystemError),
//     #[error(transparent)]
//     MessageReceiverError(MessageReceiverErrorKind),
//     // OutBoundError(OutBoundError),
//     #[error("Some Error ")]
//     SomeError,
// }

// #[derive(Debug, ThisError)]
// #[error("System error for service={service} kind={kind} \n reason={reason} \n data={data:?}")]
// pub struct SystemServiceError {
//     kind: SystemServiceErrorKind,
//     reason: String,
//     data: Option<String>,
//     service: String,
// }

// impl From<DecisionOutBoxServiceError> for SystemServiceError {
//     fn from(inner: DecisionOutBoxServiceError) -> Self {
//         match inner {
//             DecisionOutBoxServiceError::XDBError(dberror) => {
//                 if dberror.kind == DecisionStoreErrorKind::ParseError {
//                     SystemServiceError {
//                         kind: SystemServiceErrorKind::ParseError,
//                         reason: dberror.reason,
//                         data: dberror.data,
//                         service: "DecisionOutBoxService".to_string(),
//                     }
//                 } else {
//                     SystemServiceError {
//                         kind: SystemServiceErrorKind::DBError,
//                         reason: dberror.reason,
//                         data: dberror.data,
//                         service: "DecisionOutBoxService".to_string(),
//                     }
//                 }
//             }
//             DecisionOutBoxServiceError::PublishError(pub_error) => SystemServiceError {
//                 kind: SystemServiceErrorKind::SomeError,
//                 reason: pub_error.reason,
//                 data: pub_error.data,
//                 service: "DecisionOutBoxService".to_string(),
//             },
//         }
//     }
// }

// impl From<CertifierServiceError> for SystemServiceError {
//     fn from(_certifier_error: CertifierServiceError) -> Self {
//         SystemServiceError {
//             kind: SystemServiceErrorKind::CertifierError,
//             reason: "Certifier error".to_string(),
//             data: None,
//             service: "CertifierService".to_string(),
//         }
//     }
// }
// impl From<HealthCheckServiceError> for SystemServiceError {
//     fn from(_hc_error: HealthCheckServiceError) -> Self {
//         SystemServiceError {
//             kind: SystemServiceErrorKind::SomeError,
//             reason: "Health check error".to_string(),
//             data: None,
//             service: "HealthCheckService".to_string(),
//         }
//     }
// }
// impl From<MessageReceiverServiceError> for SystemServiceError {
//     fn from(_msg_rx_error: MessageReceiverServiceError) -> Self {
//         SystemServiceError {
//             kind: SystemServiceErrorKind::SomeError,
//             reason: "Message Receiver error".to_string(),
//             data: None,
//             service: "MessageReceiverService".to_string(),
//         }
//     }
// }

// #[derive(Debug, ThisError)]
// pub enum TalosError {
//     #[error(transparent)]
//     SystemError(#[from] SystemError),

//     #[error(transparent)]
//     SuffixError(#[from] SuffixError),
//     #[error("[VALIDATION ERROR] - Validation failed with reason {reason}. \n {data:?}  ")]
//     Validation {
//         // code: String,
//         reason: String,
//         data: Option<String>,
//     },

//     #[error("[MESSAGE SERVICE ERROR] - Message Service Error  {data:?} ")]
//     MessageService {
//         // code: String,
//         data: Option<String>,
//         source: KafkaAdapterError,
//     },

//     #[error("[XDB ERROR] - XDB Error ")]
//     DBError { data: Option<String>, source: PgError },

//     #[error("[UKNOWN EXCEPTION] - Unknown exception occured. {details:?} ")]
//     UnknownException {
//         // code: String,
//         details: String,
//     },
// }

// impl From<PgError> for TalosError {
//     fn from(pg_error: PgError) -> Self {
//         TalosError::DBError { data: None, source: pg_error }
//     }
// }

// impl From<KafkaAdapterError> for TalosError {
//     fn from(kafka_error: KafkaAdapterError) -> Self {
//         match kafka_error {
//             KafkaAdapterError::SubscribeTopic(_, ref topic) => TalosError::MessageService {
//                 data: Some(topic.to_string()),
//                 source: kafka_error,
//             },
//             KafkaAdapterError::Commit(_, vers) => TalosError::MessageService {
//                 data: vers.map(|v| v.to_string()),
//                 source: kafka_error,
//             },
//             KafkaAdapterError::ReceiveMessage(_) => TalosError::MessageService {
//                 data: None,
//                 source: kafka_error,
//             },
//             KafkaAdapterError::PublishMessage(_, ref msg) => TalosError::MessageService {
//                 data: Some(msg.to_string()),
//                 source: kafka_error,
//             },
//             KafkaAdapterError::UnhandledKafkaException(_) => TalosError::MessageService {
//                 data: None,
//                 source: kafka_error,
//             },
//             KafkaAdapterError::EmptyPayload => TalosError::MessageService {
//                 data: None,
//                 source: kafka_error,
//             },
//             KafkaAdapterError::HeaderNotFound(header) => TalosError::Validation {
//                 reason: "Header not found".to_string(),
//                 data: Some(header),
//             },
//             KafkaAdapterError::UnknownMessageType(msg_type) => TalosError::Validation {
//                 reason: "Unknown message type".to_string(),
//                 data: Some(msg_type),
//             },
//             KafkaAdapterError::MessageParsing(msg) => TalosError::Validation {
//                 reason: "Error parsing message".to_string(),
//                 data: Some(msg),
//             },
//             KafkaAdapterError::UnknownException(e) => TalosError::UnknownException { details: e },
//             KafkaAdapterError::OffsetZeroMessage => TalosError::Validation {
//                 reason: "Offset zero message".to_string(),
//                 data: None,
//             },
//         }
//     }
// }

// pub trait ServiceErrorTrait {}
