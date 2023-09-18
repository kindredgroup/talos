use ahash::HashMap;
use serde::{Deserialize, Deserializer, Serialize}; // 1.0.130
use serde_json::{self, Value};
use strum::{Display, EnumString}; // 1.0.67

#[derive(Debug, Display, Serialize, Deserialize, EnumString, Clone, Eq, PartialEq)]
pub enum PublishActions {
    #[serde(rename(deserialize = "kafka"))]
    // #[strum(default)]
    Kafka(Vec<KafkaActions>),
    // #[strum(serialize = "kaf2ka")]
    // // #[strum(default)]
    // Kaf2ka(Vec<KafkaActions>),
    // SNS(String),
    // unknown: String,
    // #[serde(flatten)]
    Extra(HashMap<String, Value>),
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct KafkaActions {
    pub topic: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Display, EnumString, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum OnCommitActions {
    #[serde(rename(deserialize = "publish"))]
    Publish(Option<PublishActions>),
}
