use async_trait::async_trait;
use cohort::replicator::core::{ReplicatorInstaller, StatemapItem};
use futures::future::BoxFuture;

pub struct ReplicatorInstallerImpl {
    pub external_impl: Box<dyn Fn(Vec<StatemapItem>, u64) -> BoxFuture<'static, String> + Sync + Send>,
}

#[async_trait]
impl ReplicatorInstaller for ReplicatorInstallerImpl {
    async fn install(&mut self, sm: Vec<StatemapItem>, version: Option<u64>) -> Result<bool, String> {
        let mut error = (self.external_impl)(sm, version.unwrap_or(0)).await;
        error = error.trim().into();
        if error.is_empty() {
            Ok(true)
        } else {
            Err(error)
        }
    }
}
