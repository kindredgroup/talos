use async_trait::async_trait;

#[derive(Debug, PartialEq, PartialOrd)]
pub enum CapturedState {
    Abort(String),
    Proceed(u64, Vec<CapturedItemState>),
    // pub abort_reason: Option<String>,
    // pub snapshot_version: u64,
    // pub items: Vec<CapturedItemState>,
}

#[derive(Debug, PartialEq, PartialOrd)]
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
    async fn install(&self, xid: String, safepoint: u64, new_version: u64, attempt_nr: u32) -> Result<OutOfOrderInstallOutcome, String>;
}

pub enum OutOfOrderInstallOutcome {
    Installed,
    InstalledAlready,
    SafepointCondition,
}
