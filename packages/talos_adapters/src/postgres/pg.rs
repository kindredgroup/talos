use async_trait::async_trait;
use deadpool_postgres::{Config, ManagerConfig, Object, Pool, Runtime};
use serde_json::{json, Value};
use talos_core::{
    model::DecisionMessage,
    ports::{
        common::SharedPortTraits,
        errors::{DecisionStoreError, DecisionStoreErrorKind},
        DecisionStore,
    },
};
use tokio_postgres::NoTls;

use crate::{PgConfig, PgError};

use super::utils::{get_uuid_key, parse_json_column};

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

        //test connection
        let _ = pool.get().await.map_err(PgError::GetClientFromPool)?;

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

        let key_uuid = get_uuid_key(&key)?;
        let rows = client.query_opt(&stmt, &[&key_uuid]).await.map_err(|e| DecisionStoreError {
            kind: DecisionStoreErrorKind::GetDecision,
            reason: e.to_string(),
            data: Some(key.clone()),
        })?;

        let Some(row) = rows else {
            return Ok(None);
        };

        let val = row.get::<&str, Option<Value>>("decision");
        let Some(value) = val else {
            return Ok(None);
        };

        Ok(Some(parse_json_column(&key, value)?))
    }

    async fn insert_decision(&self, key: String, decision: Self::Decision) -> Result<Self::Decision, DecisionStoreError> {
        let client = self.get_client().await.map_err(|e| DecisionStoreError {
            kind: DecisionStoreErrorKind::ClientError,
            reason: e.to_string(),
            data: None,
        })?;
        let key_uuid = get_uuid_key(&key)?;

        let stmt = client
            .prepare_cached(
                "WITH ins AS (
                    INSERT INTO xdb(xid, decision) 
                    VALUES ($1, $2)
                    ON CONFLICT DO NOTHING 
                    RETURNING xid, decision
                )
                SELECT * from ins
                UNION 
                SELECT xid, decision from xdb where xid = $1",
            )
            .await
            .map_err(|e| DecisionStoreError {
                kind: DecisionStoreErrorKind::InsertDecision,
                reason: e.to_string(),
                data: Some(key.clone()),
            })?;

        // Execute insert returning the row. If duplicate is found, return the existing row in table.
        let result = client.query_one(&stmt, &[&key_uuid, &json!(decision)]).await;
        match result {
            Ok(row) => {
                let decision = match row.get::<&str, Option<Value>>("decision") {
                    Some(value) => Ok(parse_json_column(&key, value)?),
                    _ => Err(DecisionStoreError {
                        kind: DecisionStoreErrorKind::NoRowReturned,
                        reason: "Insert did not return rows".to_owned(),
                        data: Some(key.clone()),
                    }),
                };

                return decision;
            }
            Err(e) => {
                return Err(DecisionStoreError {
                    kind: DecisionStoreErrorKind::InsertDecision,
                    reason: e.to_string(),
                    data: Some(key.clone()),
                });
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
