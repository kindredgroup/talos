use async_trait::async_trait;
use talos_cohort_replicator::StatemapItem;

pub struct CapturedState {
    pub snapshot_version: u64,
    pub items: Vec<CapturedItemState>,
}

#[derive(Debug)]
pub struct CapturedItemState {
    pub id: String,
    pub version: u64,
}

#[async_trait]
pub trait ItemStateProvider {
    async fn get_state(&self) -> Result<CapturedState, String>;
}

#[async_trait]
pub trait OutOfOrderInstaller {
    async fn install(&self, xid: String, safepoint: u64, new_version: u64, attempt_nr: u64) -> Result<OutOfOrderInstallOutcome, String>;
}

#[async_trait]
pub trait StatemapInstaller {
    async fn install(&self, statemap: Vec<StatemapItem>, snapshot_version: u64) -> Result<(), String>;
}

pub enum OutOfOrderInstallOutcome {
    Installed,
    InstalledAlready,
    SafepointCondition,
}
