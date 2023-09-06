use std::collections::HashMap;

use async_trait::async_trait;
use serde_json::Value;

#[derive(Debug, PartialEq)]
pub enum CertificationCandidateCallbackResponse {
    Cancelled(String),
    Proceed(CertificationRequestPayload),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CertificationRequestPayload {
    pub candidate: CertificationCandidate,
    pub snapshot: u64,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CertificationCandidate {
    pub readset: Vec<String>,
    pub writeset: Vec<String>,
    pub readvers: Vec<u64>,
    pub statemaps: Option<Vec<HashMap<String, Value>>>,
}

#[derive(Debug, Clone)]
pub struct OutOfOrderInstallRequest {
    pub xid: String,
    pub version: u64,
    pub safepoint: u64,
    pub statemaps: Vec<HashMap<String, Value>>,
}

#[derive(Debug, PartialEq, PartialOrd)]

pub enum OutOfOrderInstallOutcome {
    Installed,
    InstalledAlready,
    SafepointCondition,
}

// #[async_trait]
// pub trait CertificationRequestProvider {
//     async fn get_candidate_to_certify(&self) -> Result<CertificationCandidateCallbackResponse, String>;
// }

#[async_trait]
pub trait OutOfOrderInstaller {
    async fn install(&self, install_item: OutOfOrderInstallRequest) -> Result<OutOfOrderInstallOutcome, String>;
}
