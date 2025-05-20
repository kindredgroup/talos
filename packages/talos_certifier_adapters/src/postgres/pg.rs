use std::time::Duration;

use async_trait::async_trait;
use deadpool_postgres::{Config, ManagerConfig, Object, Pool, PoolConfig, PoolError, Runtime};
use serde_json::{json, Value};
use talos_certifier::{
    model::DecisionMessage,
    ports::{
        common::SharedPortTraits,
        errors::{DecisionStoreError, DecisionStoreErrorKind},
        DecisionStore,
    },
};
use tracing::{debug, error, warn};

use tokio_postgres::NoTls;

use crate::{PgConfig, PgError};

use super::utils::{get_uuid_key, parse_json_column};

#[derive(Clone)]
pub struct Pg {
    pub pool: Pool,
    /// The max times to retry any db operation. Defaults to 5.
    pub max_retries: u32,
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

        if let Some(pool_max_size) = pg_config.pool_size {
            let pool_config = PoolConfig {
                max_size: pool_max_size as usize,
                ..PoolConfig::default()
            };

            config.pool = Some(pool_config);
        }

        let pool = config.create_pool(Some(Runtime::Tokio1), NoTls).map_err(PgError::CreatePool)?;

        //test connection
        let _ = pool.get().await.map_err(PgError::GetClientFromPool)?;

        Ok(Pg {
            pool,
            max_retries: pg_config.max_retries.unwrap_or(5_u32),
        })
    }

    pub async fn get_client(&self) -> Result<Object, PgError> {
        let client = self.pool.get().await.map_err(PgError::GetClientFromPool)?;

        Ok(client)
    }

    pub async fn get_client_with_retry(&self) -> Result<Object, PgError> {
        let mut interval = tokio::time::interval(Duration::from_millis(5_000));

        loop {
            let result = self.pool.get().await;

            match result {
                Ok(pool_object) => return Ok(pool_object),
                Err(pool_error) => match pool_error {
                    PoolError::Backend(_) | PoolError::Timeout(_) => {
                        interval.tick().await;
                        warn!("Error retreiving pool object, retrying...");
                        continue;
                    }
                    _ => return Err(PgError::GetClientFromPool(pool_error)),
                },
            };
        }
    }
}

#[async_trait]
impl DecisionStore for Pg {
    type Decision = DecisionMessage;

    async fn get_decision(&self, key: String) -> Result<Option<Self::Decision>, DecisionStoreError> {
        let client = self.get_client_with_retry().await.map_err(|e| DecisionStoreError {
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
        let mut current_count = 0;

        let mut result_f: Result<DecisionMessage, DecisionStoreError> = Err(DecisionStoreError {
            kind: DecisionStoreErrorKind::InsertDecision,
            reason: format!("Max retries exhausted for key={key} to insert decision to XDB"),
            data: Some(format!("{:?}", decision)),
        });
        while current_count <= self.max_retries {
            let client_result = self.get_client_with_retry().await.map_err(|e| DecisionStoreError {
                kind: DecisionStoreErrorKind::ClientError,
                reason: format!("Failed to get client with error {}", e),
                data: None,
            });

            if let Ok(client) = client_result {
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
                        reason: format!("Failed to prepare the insert statement to XDB {}", e),
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

                        result_f = decision;
                        debug!("Exiting from okay handle of decision insert result");
                        break;
                    }
                    Err(e) => {
                        result_f = Err(DecisionStoreError {
                            kind: DecisionStoreErrorKind::InsertDecision,
                            reason: format!("Failed to insert decision into XDB with error {}", e),
                            data: Some(key.clone()),
                        });

                        error!("{result_f:#?}");
                    }
                };
            } else if let Some(client_err) = client_result.err() {
                warn!(
                    "Error getting connection from pool prior to inserting decision to XDB with reason {}",
                    client_err.to_string()
                );
                result_f = Err(client_err);
            }

            // 10 milliseconds multiplied with 2^current_count.
            // eg. if max_retries = 5, then sleep_duration_ms would be 10, 20, 40, 80, 160
            let sleep_duration_ms = 10 * 2u64.pow(current_count);
            warn!("Retrying inserting to XDB after waiting for {sleep_duration_ms}");
            tokio::time::sleep(Duration::from_millis(sleep_duration_ms)).await;
            current_count += 1;
        }

        result_f
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
