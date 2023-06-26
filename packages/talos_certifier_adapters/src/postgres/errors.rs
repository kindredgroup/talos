use deadpool_postgres::{CreatePoolError, PoolError};
use serde_json::Value;
use talos_certifier::model::DecisionMessage;
use thiserror::Error as ThisError;
use tokio_postgres::Error as TokioPostgresError;

#[derive(Debug, ThisError)]
// TODO: double check this setting
#[allow(clippy::large_enum_variant)]
pub enum PgError {
    // Deadpool errors
    #[error("Error creating pool - {0}")]
    CreatePool(#[source] CreatePoolError),
    #[error("Error getting client from pool - {0} ")]
    GetClientFromPool(#[source] PoolError),

    // Tokio-postgres errors
    #[error("Error retreiving decision - {0} ")]
    RetreiveDecision(#[source] TokioPostgresError),
    #[error("Error inserting decision {2:?} for xid {1:?} - {0}")]
    InsertDecision(#[source] TokioPostgresError, String, DecisionMessage),
    #[error("Unknown exception - {0}")]
    UnknownException(#[from] TokioPostgresError),

    // Other errors like validation or parsing
    #[error("Error creating UUID from {1} - {0}")]
    CreateUuid(#[source] uuid::Error, String),
    #[error("Error parsing decision message {1:?} from postgres to json - {0}")]
    ParseDecisionMessage(#[source] serde_json::Error, Option<Value>),
    #[error("Error a row with the same XID {0} already exists ")]
    XidAlreadyExists(String),
}
