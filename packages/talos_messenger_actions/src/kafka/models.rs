use ahash::HashMap;
use serde::{Deserialize, Serialize}; // 1.0.130
use serde_json::{self};

fn default_text_plain_encoding() -> String {
    "text/plain".to_string()
}

fn default_application_json_encoding() -> String {
    "application/json".to_string()
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct MessengerKafkaActionHeader {
    pub key_encoding: String,
    pub key: String,
    pub value_encoding: String,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaAction {
    #[serde(default)]
    pub cluster: String,
    /// Topic to publish the payload
    pub topic: String,
    /// Key encoding to be used. Defaults to `text/plain`.
    #[serde(default = "default_text_plain_encoding")]
    pub key_encoding: String,
    /// Key for the message to publish.
    pub key: Option<String>,
    /// Optional if the message should be published to a specific partition.
    pub partition: Option<i32>,
    /// Optional headers while publishing.
    pub headers: Option<HashMap<String, String>>,
    /// Key encoding to be used. Defaults to `application/json`.
    #[serde(default = "default_application_json_encoding")]
    pub value_encoding: String,
    /// Payload to publish.
    pub value: serde_json::Value,
}
