use ahash::{HashMap, HashMapExt};
use serde_json::{json, Value};

use crate::utlis::{create_whitelist_actions_from_str, get_actions_deserialised, get_allowed_commit_actions, ActionsParserConfig};

// Start - testing create_whitelist_actions_from_str function
#[test]
fn test_fn_create_whitelist_actions_from_str() {
    let config = ActionsParserConfig {
        case_sensitive: false,
        key_value_delimiter: ":",
    };

    let actions_str = "foo:test, foo:test2, bar,FOO:test3";

    let action_map = create_whitelist_actions_from_str(actions_str, &config);

    assert_eq!(action_map.len(), 2);
    assert_eq!(action_map.get("foo").unwrap().len(), 3);
    assert!(action_map.contains_key("bar"));
}
// End - testing create_whitelist_actions_from_str function

// Start - testing get_allowed_commit_actions function
#[test]
fn test_fn_get_allowed_commit_actions_allowed_actions_negative_scenarios() {
    let mut allowed_actions = HashMap::new();

    let on_commit_actions = serde_json::json!({
        "publish": {
            "kafka": [
                {
                    "_typ": "KafkaMessage",
                    "topic": "${env}.chimera.coupons", // ${env} is substituted by the Kafka provider
                    "value": "test"
                },
                {
                    "_typ": "KafkaMessage",
                    "topic": "${env}.chimera.coupons", // ${env} is substituted by the Kafka provider
                    "value": "test"
                }
            ],
            "mqtt": [
                {
                    "_typ": "Mqtt",
                    "topic": "${env}.chimera.coupons", // ${env} is substituted by the Kafka provider
                    "value": "test"
                }
            ]
        }
    });

    // When allowed action map is empty.
    let result = get_allowed_commit_actions(&on_commit_actions, &allowed_actions);
    assert!(result.is_empty());

    // When allowed action is supported type by the messenger, but the sub actions are not provided
    allowed_actions.clear();
    allowed_actions.insert("publish".to_string(), vec![]);
    let result = get_allowed_commit_actions(&on_commit_actions, &allowed_actions);
    assert!(result.is_empty());

    // When allowed action is supported type by the messenger, but the sub actions are not supported
    allowed_actions.clear();
    allowed_actions.insert("publish".to_string(), vec!["sqs".to_string(), "sns".to_string()]);
    let result = get_allowed_commit_actions(&on_commit_actions, &allowed_actions);
    assert!(result.is_empty());

    // When allowed action is non supported type by the messenger, with empty sub type
    allowed_actions.clear();
    allowed_actions.insert("random".to_string(), vec![]);
    let result = get_allowed_commit_actions(&on_commit_actions, &allowed_actions);
    assert!(result.is_empty());

    // When allowed action is non supported type by the messenger, but has valid sub actions
    allowed_actions.clear();
    allowed_actions.insert(
        "random".to_string(),
        vec!["sqs".to_string(), "sns".to_string(), "kafka".to_string(), "mqtt".to_string()],
    );
    let result = get_allowed_commit_actions(&on_commit_actions, &allowed_actions);
    assert!(result.is_empty());
}

#[test]
fn test_fn_get_allowed_commit_actions_on_commit_action_negative_scenarios() {
    let mut allowed_actions = HashMap::new();
    allowed_actions.insert(
        "publish".to_string(),
        vec!["sqs".to_string(), "sns".to_string(), "kafka".to_string(), "mqtt".to_string()],
    );

    // When on_commit_actions are not present
    let on_commit_actions = serde_json::json!({});
    let result = get_allowed_commit_actions(&on_commit_actions, &allowed_actions);
    assert!(result.is_empty());

    // When on_commit_actions is of type not supported by messenger
    // 1. When actions is an array
    let on_commit_actions = serde_json::json!([1, 2, 3, 4]);
    let result = get_allowed_commit_actions(&on_commit_actions, &allowed_actions);
    assert!(result.is_empty());

    // 2. When actions is some other object type
    let on_commit_actions = serde_json::json!({
        "test": {
            "a": "foo",
            "kafka": "bar"
        }
    });
    let result = get_allowed_commit_actions(&on_commit_actions, &allowed_actions);
    assert!(result.is_empty());

    // When on_commit_actions is valid json supported by messenger, but not the action required by messenger
    let on_commit_actions = serde_json::json!({
        "random": {
            "kafka": [
                {
                    "_typ": "KafkaMessage",
                    "topic": "${env}.chimera.coupons", // ${env} is substituted by the Kafka provider
                    "value": "test"
                },
                {
                    "_typ": "KafkaMessage",
                    "topic": "${env}.chimera.coupons", // ${env} is substituted by the Kafka provider
                    "value": "test"
                }
            ],
            "mqtt": [
                {
                    "_typ": "Mqtt",
                    "topic": "${env}.chimera.coupons", // ${env} is substituted by the Kafka provider
                    "value": "test"
                }
            ]
        }
    });
    let result = get_allowed_commit_actions(&on_commit_actions, &allowed_actions);
    assert!(result.is_empty());

    // When on_commit_actions is valid json supported by messenger, with valid action, but the sub-actions are not supported by messenger
    let on_commit_actions = serde_json::json!({
        "publish": {
            "foo": "Lorem",
            "bar": "Ipsum"
        }
    });
    let result = get_allowed_commit_actions(&on_commit_actions, &allowed_actions);
    assert!(result.is_empty());
}

#[test]
fn test_fn_get_allowed_commit_actions_positive_scenario() {
    let mut allowed_actions = HashMap::new();

    let on_commit_actions = serde_json::json!({
        "publish": {
            "kafka": [
                {
                    "_typ": "KafkaMessage",
                    "topic": "${env}.chimera.coupons", // ${env} is substituted by the Kafka provider
                    "value": "test"
                },
                {
                    "_typ": "KafkaMessage",
                    "topic": "${env}.chimera.coupons", // ${env} is substituted by the Kafka provider
                    "value": "test"
                }
            ],
            "mqtt": [
                {
                    "_typ": "Mqtt",
                    "topic": "${env}.chimera.coupons", // ${env} is substituted by the Kafka provider
                    "value": "test"
                }
            ]
        }
    });

    allowed_actions.insert(
        "publish".to_string(),
        vec!["sqs".to_string(), "sns".to_string(), "kafka".to_string(), "mqtt".to_string()],
    );
    let result = get_allowed_commit_actions(&on_commit_actions, &allowed_actions);
    assert_eq!(result.len(), 2);
}

// End - testing get_allowed_commit_actions function

// Start - testing get_actions_deserialised function

// Negative scenarios:-
// 1. Value
// . Empty or null Value
// . Array
// . String
//
// 2. Key
#[test]
fn test_fn_get_actions_deserialised_actions_incorrect_arguments() {
    let mut actions_map: HashMap<String, Value> = HashMap::new();

    // When value is empty string.
    actions_map.insert("kafka".to_string(), "".into());
    let result = get_actions_deserialised::<Vec<String>>(actions_map.get("kafka").unwrap());
    assert!(result.is_err());

    // When value is Array of string, but we want to parse it to array of u32.
    actions_map.insert("kafka".to_string(), vec!["foo", "bar"].into());
    let result = get_actions_deserialised::<Vec<u32>>(actions_map.get("kafka").unwrap());
    assert!(result.is_err());
}
#[test]
fn test_fn_get_actions_deserialised_actions_correct_arguments_passed() {
    let mut actions_map: HashMap<String, Value> = HashMap::new();

    // When value is empty string.
    actions_map.insert("kafka".to_string(), "".into());
    let result = get_actions_deserialised::<String>(actions_map.get("kafka").unwrap());
    assert!(result.is_ok());

    // When value is Array of string.
    actions_map.insert("kafka".to_string(), vec!["foo", "bar"].into());
    let result = get_actions_deserialised::<Vec<String>>(actions_map.get("kafka").unwrap());
    assert!(result.is_ok());

    // More complex type
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct Address {
        street_number: u32,
        street: String,
        city: String,
        state: String,
    }

    actions_map.insert(
        "address".to_string(),
        json!(

            {
                "street_number": 47,
                "street": "Wallaby Way".to_string(),
                "city": "Sydney".to_string(),
                "state": "New South Wales".to_string(),
            }
        ),
    );
    let result = get_actions_deserialised::<Address>(actions_map.get("address").unwrap());
    assert!(result.is_ok());
}
