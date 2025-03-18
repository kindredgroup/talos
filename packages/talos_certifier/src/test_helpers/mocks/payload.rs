use serde_json::{json, Value};

pub fn get_default_headers() -> Value {
    json!({
        "headers": {
            "correlationId": "ef4d684b-cb42-4ff3-9bca-c462699c1672",
            "version": "1",
            "type": "SomeMockTypeA",
        },
    })
}

pub fn get_default_payload() -> Value {
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
        "headers": headers.unwrap_or_else(get_default_headers),
        "key": key,
        "keyEncoding": "",
        "partition": null,
        "topic": topic,
        "value": value.unwrap_or_else(get_default_payload),
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
