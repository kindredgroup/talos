use async_trait::async_trait;
use std::fmt::Display;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use deadpool_postgres::{Config, GenericClient, ManagerConfig, Object, Pool, Runtime};
use tokio_postgres::types::ToSql;
use tokio_postgres::{NoTls, Row};

use crate::snapshot_api::SnapshotApi;
use crate::state::postgres::database_config::DatabaseConfig;

pub static SNAPSHOT_SINGLETON_ROW_ID: &str = "SINGLETON";

#[async_trait]
pub trait Action: Display {
    /// Returns the number of affected rows
    async fn execute<T>(&self, client: &T) -> Result<u64, String>
    where
        T: GenericClient + Sync;
}

pub struct Database {
    pub pool: Pool,
}

impl Database {
    async fn get(&self) -> Object {
        self.pool.get().await.unwrap()
    }

    pub async fn init_db(cfg: DatabaseConfig) -> Arc<Self> {
        let mut config = Config::new();
        config.dbname = Some(cfg.database);
        config.user = Some(cfg.user);
        config.password = Some(cfg.password);
        config.host = Some(cfg.host);
        config.port = Some(cfg.port.parse::<u16>().expect("Failed to parse port to u16"));
        config.manager = Some(ManagerConfig {
            recycling_method: deadpool_postgres::RecyclingMethod::Fast,
        });

        let pool = config
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .map_err(|e| format!("Cannot connect to database. Error: {}", e))
            .unwrap();

        //test connection
        let _ = pool.get().await.map_err(|e| format!("Cannot get client from DB pool. Error: {}", e));

        Arc::new(Database { pool })
    }

    pub async fn query_one<T>(&self, sql: &str, params: &[&(dyn ToSql + Sync)], fn_converter: fn(&Row) -> T) -> T {
        let client = self.get().await;
        let stm = client.prepare_cached(sql).await.unwrap();
        fn_converter(&client.query_one(&stm, params).await.unwrap())
    }

    pub async fn query_opt<T>(&self, sql: &str, params: &[&(dyn ToSql + Sync)], fn_converter: fn(&Row) -> T) -> Option<T> {
        let client = self.get().await;
        let stm = client.prepare_cached(sql).await.unwrap();
        let result = client.query_opt(&stm, params).await.unwrap();
        result.map(|r| fn_converter(&r))
    }

    pub async fn query<T>(&self, sql: &str, fn_converter: fn(&Row) -> T) -> Vec<T> {
        let client = self.get().await;
        let stm = client.prepare_cached(sql).await.unwrap();
        let result = client.query(&stm, &[]).await.unwrap();
        result.iter().map(fn_converter).collect::<Vec<T>>()
    }

    pub async fn execute(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> u64 {
        let client = self.get().await;
        let stm = client.prepare_cached(sql).await.unwrap();
        client.execute(&stm, params).await.unwrap()
    }

    pub async fn batch<T>(&self, batch: Vec<T>, snapshot: Option<u64>) -> Result<u64, String>
    where
        T: Action,
    {
        // We attempt to execute all actions in this batch and then track how many DB rows where affected.
        // If there were no rows updated in DB then we treat this as error.
        // In case of batch execution produced an error we rollback.
        // If rollback fails we return error describing both - the reson for batch execution error and the reason for rollback error.
        // If successfull we check whether snapshot update is required.
        // Then we udpate snapshot and commit. Or we update snapshot, fail and rollback.
        // The error handling of commit is the same as for rollabck error.

        let mut client = self.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();

        #[allow(clippy::type_complexity)]
        let batch_async: Vec<Pin<Box<dyn Future<Output = Result<u64, String>> + Send>>> = batch.iter().map(|action| action.execute(&tx)).collect();
        let action_result = futures::future::try_join_all(batch_async).await;

        if action_result.is_err() {
            let action_error = action_result.unwrap_err();
            let tx_error = tx.rollback().await.map_err(|e| e.to_string());

            if tx_error.is_err() {
                Err(format!(
                    "Cannot rollback failed action. Error: {:?}, Rollback error: {}",
                    action_error,
                    tx_error.unwrap_err(),
                ))
            } else {
                Err(format!("Cannot execute batch. Error: {:?}. Rollback.", action_error))
            }
        } else {
            let mut affected_rows = 0_u64;
            for c in action_result.unwrap().iter() {
                affected_rows += c;
            }

            if affected_rows == 0 {
                // still fail here
                let tx_error = tx.rollback().await.map_err(|e| e.to_string());
                if tx_error.is_err() {
                    Err(format!(
                        "Cannot rollback failed action. Error: No rows where updated. Rollback error: {}",
                        tx_error.unwrap_err(),
                    ))
                } else {
                    Err("Cannot execute action. Error: No rows where updated".to_string())
                }
            } else {
                if let Some(new_version) = snapshot {
                    let snapshot_update_result = SnapshotApi::update_using(&tx, new_version).await;
                    if let Ok(rows) = snapshot_update_result {
                        affected_rows += rows;
                    } else {
                        let snapshot_error = snapshot_update_result.unwrap_err();
                        // there was an error updating snapshot, we need to rollabck the whole batch
                        let tx_error = tx.rollback().await.map_err(|e| e.to_string());
                        return if tx_error.is_err() {
                            Err(format!(
                                "Cannot rollback after snapshot failed to update. Error: {}. Rollback error: {}",
                                snapshot_error,
                                tx_error.unwrap_err(),
                            ))
                        } else {
                            Err(format!("Cannot update snapshot. Error: {}. Rollback", snapshot_error))
                        };
                    }
                }

                tx.commit().await.map_err(|tx_error| format!("Commit error: {}", tx_error))?;

                Ok(affected_rows)
            }
        }
    }
}
