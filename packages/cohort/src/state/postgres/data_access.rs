use async_trait::async_trait;
use deadpool_postgres::{Object, Transaction};
use tokio_postgres::types::ToSql;

use crate::state::data_access_api::{Connection, ConnectionApi, ManualTx, TxApi};

pub struct PostgresManualTx<'a> {
    tx: Transaction<'a>,
}

#[async_trait]
impl<'a> ManualTx for PostgresManualTx<'a> {
    async fn execute(&self, sql: String, params: &[&(dyn ToSql + Sync)]) -> Result<u64, String> {
        log::debug!("execute: \n\t{}", sql.as_str());
        let statement = self.tx.prepare_cached(&sql).await.unwrap();
        self.tx.execute(&statement, params).await.map_err(|e| e.to_string())
    }

    async fn commit(self) -> Result<(), String> {
        log::debug!("commit");
        let r = self.tx.commit().await.map_err(|e| e.to_string());
        if r.is_err() {
            log::debug!("...commit fail");
        }
        r
    }

    async fn rollback(self) -> Result<(), String> {
        log::debug!("rollback...");
        let r = self.tx.rollback().await.map_err(|e| e.to_string());
        if r.is_err() {
            log::debug!("...rollback fail");
        }
        r
    }
}

pub struct PostgresAutoTx {
    pub client: Object,
}

#[async_trait]
impl Connection for PostgresAutoTx {
    async fn execute(&self, sql: String, params: &[&(dyn ToSql + Sync)]) -> Result<u64, String> {
        log::debug!("execute: \n\t{}", sql.as_str());
        let statement = self.client.prepare_cached(&sql).await.unwrap();
        self.client.execute(&statement, params).await.map_err(|e| e.to_string())
    }
}

pub struct PostgresApi {
    pub client: deadpool_postgres::Object,
}

#[async_trait]
impl<'a> TxApi<'a, PostgresManualTx<'a>> for PostgresApi {
    async fn transaction(&'a mut self) -> PostgresManualTx<'a> {
        let tx = self
            .client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::Serializable)
            .start()
            .await
            .unwrap();
        // let tx = self.client.transaction().await.unwrap();
        PostgresManualTx { tx }
    }
}

#[async_trait]
impl ConnectionApi<PostgresAutoTx> for PostgresApi {
    async fn connect(self) -> PostgresAutoTx {
        PostgresAutoTx { client: self.client }
    }
}
