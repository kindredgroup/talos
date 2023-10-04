use ahash::HashMap;
use serde::{Deserialize, Serialize}; // 1.0.130
use serde_json::{self};

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct KafkaAction {
    // TODO: GK - Add additional Kafka producer related props here.
    // pub version: u32,
    // pub cluster: String,
    pub topic: String,
    pub key: Option<String>,
    pub partition: Option<i32>,
    pub headers: Option<HashMap<String, String>>,
    pub value: serde_json::Value,
}
