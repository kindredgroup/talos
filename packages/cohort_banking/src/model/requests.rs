use serde_json::Value;
use std::collections::HashMap;

#[derive(Clone)]
pub struct CandidateData {
    pub readset: Vec<String>,
    pub writeset: Vec<String>,
    pub statemap: Option<Vec<HashMap<String, Value>>>,
    // The "snapshot" is intentionally messing here. We will compute it ourselves before feeding this data to Talos
}

#[derive(Clone)]
pub struct CertificationRequest {
    pub candidate: CandidateData,
    pub timeout_ms: u64,
}
