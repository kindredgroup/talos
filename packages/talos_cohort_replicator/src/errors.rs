use thiserror::Error as ThisError;

// enum ServiceErrorKind {
//     // i/o errors
//     DBError,
//     EventStreamError,
//     // internal channel errors
//     ChannelError,
//     // general system related errors
//     SystemError,
// }

#[derive(Debug, ThisError, PartialEq, Clone)]
#[error("error reason={reason} ")]
pub struct ServiceError {
    // pub kind: SystemServiceErrorKind,
    pub reason: String,
    // pub data: Option<String>,
    // pub service: String,
}
