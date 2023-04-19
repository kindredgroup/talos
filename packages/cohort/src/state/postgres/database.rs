use std::sync::Arc;

use deadpool_postgres::{Config, ManagerConfig, Object, Pool, Runtime};
use tokio_postgres::types::ToSql;
use tokio_postgres::{NoTls, Row};

use crate::state::postgres::database_config::DatabaseConfig;

pub static SNAPSHOT_SINGLETON_ROW_ID: &str = "SINGLETON";

pub struct Database {
    pub pool: Pool,
}

impl Database {
    async fn get(&self) -> Object {
        let client = self.pool.get().await.unwrap();
        client
    }

    pub async fn init_db(cfg: DatabaseConfig) -> Arc<Database> {
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
        let stm = client.prepare(sql).await.unwrap();
        fn_converter(&client.query_one(&stm, params).await.unwrap())
    }

    pub async fn query_opt<T>(&self, sql: &str, params: &[&(dyn ToSql + Sync)], fn_converter: fn(&Row) -> T) -> Option<T> {
        let client = self.get().await;
        let stm = client.prepare(sql).await.unwrap();
        let result = client.query_opt(&stm, params).await.unwrap();
        result.map(|r| fn_converter(&r))
    }

    pub async fn query<T>(&self, sql: &str, fn_converter: fn(&Row) -> T) -> Vec<T> {
        let client = self.get().await;
        let stm = client.prepare(sql).await.unwrap();
        let result = client.query(&stm, &[]).await.unwrap();
        result.iter().map(fn_converter).collect::<Vec<T>>()
    }
}
