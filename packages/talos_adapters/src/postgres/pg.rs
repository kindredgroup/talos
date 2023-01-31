use async_trait::async_trait;
use deadpool_postgres::{Config, ManagerConfig, Object, Pool, Runtime};
use serde_json::{json, Value};
use talos_core::{
    model::decision_message::DecisionMessage,
    ports::{
        common::SharedPortTraits,
        decision_store::DecisionStore,
        errors::{DecisionStoreError, DecisionStoreErrorKind},
    },
};
use tokio_postgres::{error::SqlState, NoTls};
use uuid::Uuid;

use crate::{PgConfig, PgError};

#[derive(Clone)]
pub struct Pg {
    pub pool: Pool,
}
// create postgres client
impl Pg {
    pub async fn new(pg_config: PgConfig) -> Result<Self, PgError> {
        let mut config = Config::new();
        config.dbname = Some(pg_config.database);
        config.user = Some(pg_config.user);
        config.password = Some(pg_config.password);
        config.host = Some(pg_config.host);
        config.port = Some(pg_config.port.parse::<u16>().expect("Failed to parse port to u16"));
        config.manager = Some(ManagerConfig {
            recycling_method: deadpool_postgres::RecyclingMethod::Fast,
        });

        let pool = config.create_pool(Some(Runtime::Tokio1), NoTls).map_err(PgError::CreatePool)?;

        Ok(Pg { pool })
    }

    pub async fn get_client(&self) -> Result<Object, PgError> {
        let client = self.pool.get().await.map_err(PgError::GetClientFromPool)?;

        Ok(client)
    }
}

#[async_trait]
impl DecisionStore for Pg {
    type Decision = DecisionMessage;

    async fn get_decision(&self, key: String) -> Result<Option<Self::Decision>, DecisionStoreError> {
        let client = self.get_client().await.map_err(|e| DecisionStoreError {
            kind: DecisionStoreErrorKind::ClientError,
            reason: e.to_string(),
            data: None,
        })?;
        let stmt = client.prepare_cached("SELECT xid, decision from xdb where xid = $1").await.unwrap();

        let key_uuid = Uuid::parse_str(&key).map_err(|e| DecisionStoreError {
            kind: DecisionStoreErrorKind::CreateKey,
            reason: e.to_string(),
            data: Some(key.clone()),
        })?;
        let rows = client.query_opt(&stmt, &[&key_uuid]).await.map_err(|e| DecisionStoreError {
            kind: DecisionStoreErrorKind::GetDecision,
            reason: e.to_string(),
            data: Some(key.clone()),
        })?;

        if let Some(row) = rows {
            let val = row.get::<&str, Option<Value>>("decision");

            return match val {
                Some(Value::Object(x)) => {
                    let decision = serde_json::from_value::<Self::Decision>(Value::Object(x)).map_err(|e| DecisionStoreError {
                        kind: DecisionStoreErrorKind::ParseError,
                        reason: e.to_string(),
                        data: Some(key),
                    })?;
                    return Ok(Some(decision));
                }
                _ => Ok(None),
            };
        };
        Ok(None)
    }

    async fn insert_decision(&mut self, key: String, decision: Self::Decision) -> Result<Option<Self::Decision>, DecisionStoreError> {
        let client = self.get_client().await.map_err(|e| DecisionStoreError {
            kind: DecisionStoreErrorKind::ClientError,
            reason: e.to_string(),
            data: None,
        })?;
        let key_uuid = Uuid::parse_str(&key).map_err(|e| DecisionStoreError {
            kind: DecisionStoreErrorKind::CreateKey,
            reason: e.to_string(),
            data: Some(key.clone()),
        })?;

        let stmt = client
            .prepare_cached("INSERT INTO xdb (xid, decision) VALUES ($1, $2)")
            .await
            .map_err(|e| DecisionStoreError {
                kind: DecisionStoreErrorKind::InsertDecision,
                reason: e.to_string(),
                data: Some(key.clone()),
            })?;

        let result = client.execute(&stmt, &[&key_uuid, &json!(decision)]).await;
        match result {
            Ok(_) => Ok(None),
            Err(e) => {
                if let Some(&SqlState::UNIQUE_VIOLATION) = e.code() {
                    return self.get_decision(key).await;
                } else {
                    return Err(DecisionStoreError {
                        kind: DecisionStoreErrorKind::InsertDecision,
                        reason: e.to_string(),
                        data: Some(key.clone()),
                    });
                }
            }
        }

        // Ok(())
    }
}

#[async_trait]
impl SharedPortTraits for Pg {
    async fn is_healthy(&self) -> bool {
        //TODO - Implement health check here
        true
    }

    async fn shutdown(&self) -> bool {
        self.pool.close();

        self.pool.is_closed()
    }
}
