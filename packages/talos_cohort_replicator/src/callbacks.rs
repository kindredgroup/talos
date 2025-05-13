use async_trait::async_trait;

use crate::StatemapItem;

#[async_trait]
pub trait ReplicatorInstaller {
    async fn install(&self, sm: Vec<StatemapItem>, version: u64) -> Result<(), String>;
}
#[async_trait]
pub trait ReplicatorSnapshotProvider {
    async fn update_snapshot(&self, version: u64) -> Result<(), String>;
    async fn get_snapshot(&self) -> Result<u64, String>;
}
