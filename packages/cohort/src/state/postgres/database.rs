use async_trait::async_trait;

use std::fmt::Display;
use std::sync::Arc;

use tokio_postgres::types::ToSql;
use tokio_postgres::{NoTls, Row};

use deadpool_postgres::{Config, GenericClient, ManagerConfig, Object, Pool, Runtime};

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
}
