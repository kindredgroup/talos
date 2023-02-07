use std::{collections::HashMap, str::FromStr};

use rdkafka::{
    message::{BorrowedMessage, Header, Headers, OwnedHeaders},
    Message,
};
use serde::de::DeserializeOwned;
use talos_core::{core::MessageVariant, errors::CommonError};

/// Builds a map of headers for the received Kafka message
pub fn get_message_headers(message: &BorrowedMessage) -> Option<HashMap<String, String>> {
    if let Some(headers) = message.headers() {
        let headers = (0..headers.count()).fold(HashMap::<String, String>::new(), |mut acc, i| {
            if let (k, Some(v)) = (headers.get(i).key, headers.get(i).value) {
                acc.insert(k.to_owned(), String::from_utf8_lossy(v).into_owned());
            }

            acc
        });

        if headers.is_empty() {
            return None;
        } else {
            return Some(headers);
        }
    }

    None
}

pub fn kafka_topic_prefixed(topic: &str, prefix: &str) -> String {
    format!("{}{}", prefix, topic)
}

pub fn build_kafka_headers(headers: HashMap<String, String>) -> OwnedHeaders {
    let owned_headers = OwnedHeaders::new();

    let owned_headers = headers.iter().fold(owned_headers, |acc, x| {
        let header = Header { key: x.0, value: Some(x.1) };
        acc.insert(header)
    });

    owned_headers
}

///  Parses the payload message from Kafka into the struct defined by the generic.
///
///  returns a result of either `T` or `ParseError`
pub fn parse_kafka_payload<T: DeserializeOwned>(message: &[u8]) -> Result<T, CommonError> {
    serde_json::from_slice::<T>(message).map_err(|err| CommonError::ParseError {
        data: String::from_utf8_lossy(message).into_owned(),
        reason: err.to_string(),
    })
}

///  Parses the message type string to enum MessageVariant.
///
///  returns a result of either `MessageVariant` enum or `ParseError`
pub fn parse_message_variant(message_type: &String) -> Result<MessageVariant, CommonError> {
    MessageVariant::from_str(message_type).map_err(|e| CommonError::ParseError {
        reason: e.to_string(),
        data: message_type.to_string(),
    })
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use rdkafka::message::Headers;
    use serde::Deserialize;
    use talos_core::{core::MessageVariant, errors::CommonError};

    use crate::kafka::utils::parse_kafka_payload;

    use super::{build_kafka_headers, kafka_topic_prefixed, parse_message_variant};

    #[test]
    fn test_kafka_topic_prefix() {
        assert_eq!(kafka_topic_prefixed("topic", "prefix-"), "prefix-topic");
    }

    #[test]
    fn test_parse_kafka_payload_successfully() {
        #[derive(Deserialize)]
        struct User {
            name: String,
            age: u32,
        }

        let json_user_data_as_str = r#"
        {
            "name": "John Doe",
            "age": 43
        }"#;

        let json_user_data_as_u8array = json_user_data_as_str.as_bytes();

        let parse_result = parse_kafka_payload::<User>(json_user_data_as_u8array).unwrap();

        assert_eq!(parse_result.name, "John Doe".to_owned());
        assert_eq!(parse_result.age, 43);
    }

    #[test]
    fn test_parse_kafka_payload_error_parsing() {
        #[derive(Deserialize, Debug)]
        struct User {
            _name: String,
            _age: u32,
        }

        let json_user_data_as_str = "Hello World";

        let json_user_data_as_u8array = json_user_data_as_str.as_bytes();

        let parse_error = parse_kafka_payload::<User>(json_user_data_as_u8array).unwrap_err();

        assert!(matches!(parse_error, CommonError::ParseError { .. }));
    }

    #[test]
    fn test_parse_message_variant_successfully() {
        let parse_candidate_variant = parse_message_variant(&"Candidate".to_string()).unwrap();
        assert_eq!(parse_candidate_variant, MessageVariant::Candidate);

        let parse_decision_variant = parse_message_variant(&"Decision".to_string()).unwrap();
        assert_eq!(parse_decision_variant, MessageVariant::Decision);
    }

    #[test]
    fn test_parse_message_variant_error() {
        let parse_error = parse_message_variant(&"Error type".to_string()).unwrap_err();
        assert!(matches!(parse_error, CommonError::ParseError { .. }));
    }

    #[test]
    fn test_building_kafka_headers_correctly() {
        let mut header_hashmap = HashMap::new();
        header_hashmap.insert("header1".to_owned(), "value1".to_owned());

        // test correct header is returned from index
        let owned_headers_result = build_kafka_headers(header_hashmap.clone());
        assert_eq!(owned_headers_result.get(0).key, "header1".to_owned());

        //test the count is correct.
        assert_eq!(build_kafka_headers(header_hashmap.clone()).count(), 1);
    }
}
