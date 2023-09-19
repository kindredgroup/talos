use ahash::HashMap;
use serde::{Deserialize, Serialize}; // 1.0.130
use serde_json::{self};
use strum::{Display, EnumString}; // 1.0.67

#[derive(Debug, Display, Serialize, Deserialize, EnumString, Clone, Eq, PartialEq)]
pub enum PublishActions {
    #[serde(rename(deserialize = "kafka"))]
    // #[strum(default)]
    Kafka(Vec<KafkaAction>),
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct KafkaAction {
    pub topic: String,
    pub key: Option<String>,
    pub partition: Option<i32>,
    pub headers: Option<HashMap<String, String>>,
    pub value: serde_json::Value,
}

#[derive(Debug, Display, EnumString, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum OnCommitActions {
    #[serde(rename(deserialize = "publish"))]
    Publish(Option<PublishActions>),
}
