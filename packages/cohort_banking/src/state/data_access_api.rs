use async_trait::async_trait;
use tokio_postgres::types::ToSql;

#[async_trait]
pub trait ManualTx: Sync {
    async fn execute<'a, 'b, 'c>(&'a self, sql: String, params: &'b [&'c (dyn ToSql + Sync + 'c)]) -> Result<u64, String>;
    async fn rollback(self) -> Result<(), String>;
    async fn commit(self) -> Result<(), String>;
}

#[async_trait]
pub trait Connection: Sync {
    async fn execute(&self, sql: String, params: &[&(dyn ToSql + Sync)]) -> Result<u64, String>;
}

#[async_trait]
pub trait TxApi<'a, T: ManualTx> {
    async fn transaction(&'a mut self) -> T;
}

#[async_trait]
pub trait ConnectionApi<T: Connection> {
    async fn connect(self) -> T;
}
