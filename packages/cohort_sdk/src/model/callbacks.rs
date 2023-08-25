use async_trait::async_trait;
use talos_agent::api::StateMap;

use super::CertificationRequestPayload;

#[derive(Debug, PartialEq, PartialOrd)]
pub struct CapturedState {
    pub snapshot_version: u64,
    pub items: Vec<CapturedItemState>,
}
#[derive(Debug, PartialEq)]
pub enum CertificationCandidateCallbackResponse {
    Cancelled(String),
    Proceed(CertificationRequestPayload),
}

#[derive(Debug, PartialEq, PartialOrd)]
pub struct CapturedItemState {
    pub id: String,
    pub version: u64,
}

#[async_trait]
pub trait OutOfOrderInstaller {
    async fn install(&self, xid: &str, safepoint: &u64, version: &u64, statemaps: &StateMap, attempt_nr: u32) -> Result<OutOfOrderInstallOutcome, String>;
}

#[derive(Debug, PartialEq, PartialOrd)]

pub enum OutOfOrderInstallOutcome {
    Installed,
    InstalledAlready,
    SafepointCondition,
}
