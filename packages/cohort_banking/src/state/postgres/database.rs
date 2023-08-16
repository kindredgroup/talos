use std::fmt::{self, Display, Formatter};
// $coverage:ignore-start
use std::sync::Arc;

use strum::Display;
use tokio_postgres::types::ToSql;
use tokio_postgres::{NoTls, Row};

use deadpool_postgres::{Config, CreatePoolError, GenericClient, ManagerConfig, Object, Pool, PoolConfig, Runtime};

use crate::state::postgres::database_config::DatabaseConfig;

pub const SNAPSHOT_SINGLETON_ROW_ID: &str = "SINGLETON";

pub struct Database {
    pub pool: Arc<Pool>,
}

#[derive(Display, Debug)]
pub enum DatabaseErrorKind {
    PoolInit,
    BorrowConnection,
    QueryOrExecute,
    PrepareStatement,
    Deserialise,
}

#[derive(Debug)]
pub struct DatabaseError {
    kind: DatabaseErrorKind,
    pub reason: String,
    pub cause: Option<String>,
}

impl DatabaseError {
    pub fn cannot_borrow(cause: String) -> Self {
        Self {
            kind: DatabaseErrorKind::BorrowConnection,
            reason: "Cannot get client from DB pool.".into(),
            cause: Some(cause),
        }
    }

    pub fn query(cause: String, query: String) -> Self {
        Self {
            kind: DatabaseErrorKind::QueryOrExecute,
            cause: Some(cause),
            reason: format!("Error executing: '{}'", query),
        }
    }

    pub fn prepare(cause: String, query: String) -> Self {
        Self {
            kind: DatabaseErrorKind::PrepareStatement,
            cause: Some(cause),
            reason: format!("Error preparing statement for: '{}'", query),
        }
    }

    pub fn deserialise_payload(cause: String, message: String) -> Self {
        Self {
            kind: DatabaseErrorKind::Deserialise,
            cause: Some(cause),
            reason: format!("Resultset parsing error. Details: '{}'", message),
        }
    }
}

impl From<CreatePoolError> for DatabaseError {
    fn from(value: CreatePoolError) -> Self {
        Self {
            kind: DatabaseErrorKind::PoolInit,
            reason: "Cannot create DB pool".into(),
            cause: Some(value.to_string()),
        }
    }
}

impl Display for DatabaseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DatabaseError: [kind: {}, reason: {}, cause: {}]",
            self.kind,
            self.reason,
            self.cause.clone().unwrap_or("".into())
        )
    }
}

impl Database {
    pub async fn get(&self) -> Result<Object, DatabaseError> {
        self.pool.get().await.map_err(|e| DatabaseError::cannot_borrow(e.to_string()))
    }

    pub async fn init_db(cfg: DatabaseConfig) -> Result<Arc<Self>, DatabaseError> {
        let mut config = Config::new();
        config.dbname = Some(cfg.database);
        config.user = Some(cfg.user);
        config.password = Some(cfg.password);
        config.host = Some(cfg.host);
        config.port = Some(cfg.port.parse::<u16>().expect("Failed to parse port to u16"));
        config.manager = Some(ManagerConfig {
            recycling_method: deadpool_postgres::RecyclingMethod::Fast,
        });
        let pc = PoolConfig {
            max_size: cfg.pool_size as usize,
            ..PoolConfig::default()
        };
        config.pool = Some(pc);

        let pool = config
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .map_err(|e| format!("Cannot connect to database. Error: {}", e))
            .unwrap();

        {
            let mut tmp_list: Vec<Object> = Vec::new();
            for _ in 1..=pc.max_size {
                let client = pool.get().await.map_err(|e| DatabaseError::cannot_borrow(e.to_string()))?;

                let stm = "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ;";
                client.execute(stm, &[]).await.map_err(|e| DatabaseError::query(e.to_string(), stm.into()))?;
                tmp_list.push(client);
            }
        }

        for _ in 1..=pc.max_size {
            let client = pool.get().await.map_err(|e| DatabaseError::cannot_borrow(e.to_string()))?;

            let stm = "show transaction_isolation";
            let rs = client.query_one(stm, &[]).await.map_err(|e| DatabaseError::query(e.to_string(), stm.into()))?;
            let value: String = rs.get(0);
            log::debug!("init: db-isolation-level: {}", value);
        }

        Ok(Arc::new(Database { pool: Arc::new(pool) }))
    }

    pub async fn query_one<T>(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
        fn_converter: fn(&Row) -> Result<T, DatabaseError>,
    ) -> Result<T, DatabaseError> {
        let client = self.get().await?;
        let stm = client
            .prepare_cached(sql)
            .await
            .map_err(|e| DatabaseError::prepare(e.to_string(), sql.to_string()))?;
        fn_converter(
            &client
                .query_one(&stm, params)
                .await
                .map_err(|e| DatabaseError::query(e.to_string(), sql.into()))?,
        )
    }

    pub async fn query_opt<T>(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
        fn_converter: fn(&Row) -> Result<T, DatabaseError>,
    ) -> Result<Option<T>, DatabaseError> {
        let client = self.get().await?;
        let stm = client
            .prepare_cached(sql)
            .await
            .map_err(|e| DatabaseError::prepare(e.to_string(), sql.to_string()))?;
        let result = client
            .query_opt(&stm, params)
            .await
            .map_err(|e| DatabaseError::query(e.to_string(), sql.to_string()))?;

        if let Some(row) = result {
            fn_converter(&row).map(|v| Some(v))
        } else {
            Ok(None)
        }
    }

    pub async fn query<T>(&self, sql: &str, fn_converter: fn(&Row) -> Result<T, DatabaseError>) -> Result<Vec<T>, DatabaseError> {
        let client = self.get().await?;
        let stm = client
            .prepare_cached(sql)
            .await
            .map_err(|e| DatabaseError::prepare(e.to_string(), sql.to_string()))?;
        let result = client.query(&stm, &[]).await.map_err(|e| DatabaseError::query(e.to_string(), sql.into()))?;

        let mut items: Vec<T> = Vec::new();
        for row in result.iter() {
            items.push(fn_converter(row)?);
        }
        Ok(items)
    }

    pub async fn query_many<T>(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
        fn_converter: fn(&Row) -> Result<T, DatabaseError>,
    ) -> Result<Vec<T>, DatabaseError> {
        let client = self.get().await?;
        let stm = client
            .prepare_cached(sql)
            .await
            .map_err(|e| DatabaseError::prepare(e.to_string(), sql.to_string()))?;
        let result = client.query(&stm, params).await.map_err(|e| DatabaseError::query(e.to_string(), sql.into()))?;

        let mut items: Vec<T> = Vec::new();
        for row in result.iter() {
            items.push(fn_converter(row)?);
        }
        Ok(items)
    }

    pub async fn execute(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64, DatabaseError> {
        let client = self.get().await?;
        let stm = client
            .prepare_cached(sql)
            .await
            .map_err(|e| DatabaseError::prepare(e.to_string(), sql.to_string()))?;
        client.execute(&stm, params).await.map_err(|e| DatabaseError::query(e.to_string(), sql.into()))
    }
}
// $coverage:ignore-end
