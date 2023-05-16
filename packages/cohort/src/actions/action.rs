use std::fmt::Display;

use async_trait::async_trait;

use crate::state::data_access_api::{Connection, ManualTx};

#[async_trait]
pub trait Action: Display {
    /// Returns the number of affected rows
    async fn execute<T: ManualTx>(&self, client: &T) -> Result<u64, String>;
    async fn execute_in_db<T: Connection>(&self, client: &T) -> Result<u64, String>;
    async fn update_version<T: ManualTx>(&self, client: &T) -> Result<u64, String>;
}
