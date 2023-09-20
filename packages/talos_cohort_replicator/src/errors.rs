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

// enum ReplicatorInstallerError {
//     DBConnectionError
//     DB
//     // DB Connection Error
//     // DB Transaction Error
//     // Data deserialization Error
//     // Update table - Cohort related or snapshot ([table name], retry count)
//     // Exhausted retry and no install

// }

#[derive(strum::Display, Debug, Clone)]
pub enum ReplicatorErrorKind {
    Messaging,
    Internal,
    Persistence,
}

#[derive(Debug, Clone)]
pub struct ReplicatorError {
    pub kind: ReplicatorErrorKind,
    pub reason: String,
    pub cause: Option<String>,
}
