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

    pub async fn query_one(client: &Object, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Row {
        let stm = client.prepare(sql).await.unwrap();
        client.query_one(&stm, params).await.unwrap()
    }

    pub async fn query_opt(client: &Object, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Option<Row> {
        let stm = client.prepare(sql).await.unwrap();
        client.query_opt(&stm, params).await.unwrap()
    }

    pub async fn query(client: &Object, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Vec<Row> {
        let stm = client.prepare(sql).await.unwrap();
        client.query(&stm, params).await.unwrap()
    }
}
