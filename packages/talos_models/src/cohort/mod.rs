use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
pub struct StatemapItem {
    pub action: String,
    pub version: u64,
    pub safepoint: Option<u64>,
    pub payload: Value,
}

impl StatemapItem {
    pub fn new(action: String, version: u64, payload: Value, safepoint: Option<u64>) -> Self {
        StatemapItem {
            action,
            version,
            payload,
            safepoint,
        }
    }
}
