use deadpool_postgres::{CreatePoolError, PoolError};
use serde_json::Value;
use talos_core::model::DecisionMessage;
use thiserror::Error as ThisError;
use tokio_postgres::Error as TokioPostgresError;

#[derive(Debug, ThisError)]
pub enum PgError {
    // Deadpool errors
    #[error("Error creating pool ")]
    CreatePool(#[source] CreatePoolError),
    #[error("Error getting client from pool ")]
    GetClientFromPool(#[source] PoolError),

    // Tokio-postgres errors
    #[error("Error retreiving decision ")]
    RetreiveDecision(#[source] TokioPostgresError),
    #[error("Error inserting decision {2:?} for xid {1:?} ")]
    InsertDecision(#[source] TokioPostgresError, String, DecisionMessage),
    #[error("Unknown exception")]
    UnknownException(#[from] TokioPostgresError),

    // Other errors like validation or parsing
    #[error("Error creating UUID from {1}")]
    CreateUuid(#[source] uuid::Error, String),
    #[error("Error parsing decision message {1:?} from postgres to json")]
    ParseDecisionMessage(#[source] serde_json::Error, Option<Value>),
    #[error("Error a row with the same XID {0} already exists ")]
    XidAlreadyExists(String),
}
