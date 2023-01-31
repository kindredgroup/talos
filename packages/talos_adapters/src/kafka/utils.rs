use std::collections::HashMap;

use rdkafka::{
    message::{BorrowedMessage, Header, Headers, OwnedHeaders},
    Message,
};

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

#[cfg(test)]
mod tests {

    use super::kafka_topic_prefixed;

    #[test]
    fn test_kafka_topic_prefix() {
        assert_eq!(kafka_topic_prefixed("topic", "prefix-"), "prefix-topic");
    }
    // #[test]
    // fn test_building_kafka_headers_correctly() {
    //     let owned_headers = OwnedHeaders::new();
    //     let owned_headers = owned_headers.add("header1", "value1");
    //     let owned_headers = owned_headers.add("header2", "value2");

    //     let mut hm = HashMap::new();
    //     hm.insert("header1".to_owned(), "value1".to_owned());
    //     hm.insert("header2".to_owned(), "value2".to_owned());

    //     // test correct header is returned from index
    //     assert_eq!(build_kafka_headers(hm.clone()).get(0).unwrap().0, "header1".to_owned());
    //     assert_eq!(build_kafka_headers(hm.clone()).get(1).unwrap().0, "header2".to_owned());

    //     // // test None is returned if index
    //     // assert_eq!(build_kafka_headers(hm.clone()).get(3), None);
    //     // test correct header as the expected result
    //     assert_eq!(build_kafka_headers(hm.clone()).get(0), owned_headers.get(0));
    //     //test the count is correct.
    //     assert_eq!(build_kafka_headers(hm.clone()).count(), 2);
    // }
}
