use crate::{
    model::callbacks::StatemapInstaller,
    replicator::core::{ReplicatorInstaller, StatemapItem},
};
use async_trait::async_trait;

pub struct ReplicatorInstallerImpl<S: StatemapInstaller + Sync + Send> {
    pub installer_impl: S,
}

#[async_trait]
impl<S: StatemapInstaller + Sync + Send> ReplicatorInstaller for ReplicatorInstallerImpl<S> {
    async fn install(&mut self, sm: Vec<StatemapItem>, version: Option<u64>) -> Result<bool, String> {
        let _ = self.installer_impl.install(sm, version.unwrap_or(0)).await?;
        Ok(true)
    }
}
