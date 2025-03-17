use std::str::FromStr;

use ahash::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

pub fn get_default_kafka_headers() -> Value {
    json!({
        "headers": {
            "correlationId": "ef4d684b-cb42-4ff3-9bca-c462699c1672",
            "version": "1",
            "type": "KafkaTopicSubType",
        },
    })
}

pub fn get_default_kafka_payload() -> Value {
    json!({
        "firstName": "abc",
        "lastName": "xyz",
        "address": "42 Wallaby Way, Sydney, New South Wales, Australia",
        "description": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor
                        incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip
                        ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.
                        Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."
    })
}

pub fn build_kafka_on_commit_message(topic: &str, key: &str, value: Option<Value>, headers: Option<Value>) -> Value {
    json!({
        "cluster": "",
        "headers": headers.unwrap_or_else(get_default_kafka_headers),
        "key": key,
        "keyEncoding": "",
        "partition": null,
        "topic": topic,
        "value": value.unwrap_or_else(get_default_kafka_payload),
        "valueEncoding": ""
    })
}

pub fn build_on_commit_publish_kafka_payload(kafka_payload: Vec<Value>) -> Value {
    json!({
        "publish": {
            "kafka": kafka_payload
        }
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MockOnCommitKafkaMessage {
    /// Topic to publish the payload
    pub topic: String,
    /// Key for the message to publish.
    pub key: Option<String>,
    /// Optional headers while publishing.
    pub headers: Option<HashMap<String, String>>,
    /// Payload to publish.
    pub value: serde_json::Value,
}

impl MockOnCommitKafkaMessage {
    fn new(topic: String, key: Option<String>, headers: Option<HashMap<String, String>>, value: Value) -> Self {
        MockOnCommitKafkaMessage { topic, key, headers, value }
    }
}

impl Default for MockOnCommitKafkaMessage {
    fn default() -> Self {
        let uuid_key = Uuid::new_v4().to_string();
        Self {
            topic: "some-topic".to_string(),
            key: Some(uuid_key),
            headers: Some(serde_json::from_value(get_default_kafka_headers()).unwrap()),
            value: get_default_kafka_payload(),
        }
    }
}

impl FromStr for MockOnCommitKafkaMessage {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s).map_err(|e| e.to_string())?
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct MockOnCommitPublish {
    kafka: Vec<MockOnCommitKafkaMessage>,
}

impl MockOnCommitPublish {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_default_kafka_messages(&mut self, message_count: u32) {
        for _i in 0..message_count {
            self.kafka.push(MockOnCommitKafkaMessage::default());
        }
    }
}

impl FromStr for MockOnCommitPublish {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s).map_err(|e| e.to_string())?
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MockOnCommitMessage {
    publish: Option<MockOnCommitPublish>,
    #[serde(flatten)]
    unsupported: Value,
}

impl MockOnCommitMessage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn build_from_str(s: &str) -> Self {
        serde_json::from_str(s).unwrap()
    }

    pub fn insert_kafka_message(&mut self, topic: String, key: Option<String>, headers: Option<HashMap<String, String>>, value: Value) {
        let kafka_message = MockOnCommitKafkaMessage::new(topic, key, headers, value);
        if let Some(publish) = self.publish.as_mut() {
            publish.kafka.push(kafka_message);
        } else {
            self.publish = Some(MockOnCommitPublish { kafka: vec![kafka_message] });
        }
    }

    pub fn bulk_insert_kafka_message<F>(&mut self, f: Vec<F>)
    // where F: Fn(String, Option<String>, Option<HashMap<String, String>>, Value) -> MockOnCommitKafkaMessage
    where
        F: FnOnce() -> MockOnCommitKafkaMessage,
    {
        let mut kafka_message_vec = vec![];

        for kafka_msg_fn in f {
            let kafka_msg = kafka_msg_fn();
            kafka_message_vec.push(kafka_msg);
        }

        if let Some(publish) = self.publish.as_mut() {
            publish.kafka.extend(kafka_message_vec);
        } else {
            self.publish = Some(MockOnCommitPublish { kafka: kafka_message_vec });
        }
    }

    pub fn as_value(&self) -> Value {
        serde_json::to_value(self.clone()).unwrap()
    }
}
impl Default for MockOnCommitMessage {
    fn default() -> Self {
        Self {
            publish: None,
            unsupported: Value::Null,
        }
    }
}

// impl FromStr for MockOnCommitMessage {
//     type Err = String;

//     fn from_str(s: &str) -> Result<Self, Self::Err> {
//         serde_json::from_str(s).map_err(|e| e.to_string())?
//     }
// }
