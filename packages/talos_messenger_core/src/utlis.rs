use std::any::type_name;

use ahash::{HashMap, HashMapExt};
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{errors::MessengerActionError, suffix::AllowedActionsMapItem};

#[derive(Debug, Clone, Copy)]
pub struct ActionsParserConfig<'a> {
    pub case_sensitive: bool,
    pub key_value_delimiter: &'a str,
}

impl Default for ActionsParserConfig<'_> {
    fn default() -> Self {
        Self {
            case_sensitive: Default::default(),
            key_value_delimiter: ":",
        }
    }
}

/// Builds the list of items from a comma separated string.
///
///
/// `ActionsParserConfig` can be passed to specify if
/// - The key and value should be case sensitive. (defaults to false)
/// - Key-Value delimiter. (defaults to ':')
pub fn create_whitelist_actions_from_str(whitelist_str: &str, config: &ActionsParserConfig) -> HashMap<String, Vec<String>> {
    whitelist_str.split(',').fold(HashMap::new(), |mut acc_map, item| {
        // case sensitive check.
        let item_cased = if !config.case_sensitive { item.to_lowercase() } else { item.to_string() };
        // Has a key-value pair
        if let Some((key, value)) = item_cased.trim().split_once(config.key_value_delimiter) {
            let key_to_check = if !config.case_sensitive { key.to_lowercase() } else { key.to_owned() };
            let key_to_check = key_to_check.trim();
            // update existing entry
            if let Some(map_item) = acc_map.get_mut(key_to_check) {
                // Check for duplicate before inserting
                let value_trimmed = value.trim().to_owned();
                if !map_item.contains(&value_trimmed) {
                    map_item.push(value_trimmed)
                }
            }
            // insert new entry
            else {
                // Empty value will not be inserted
                if !value.is_empty() {
                    acc_map.insert(key.to_owned(), vec![value.to_owned()]);
                }
            }
        }
        // just key type.
        else {
            let insert_key = if config.case_sensitive {
                item_cased.to_lowercase()
            } else {
                item_cased.to_owned()
            };
            let key_to_check = insert_key.trim();
            acc_map.insert(key_to_check.trim().to_owned(), vec![]);
        }
        acc_map
    })
}

/// Retrieves the serde_json::Value for a given key
pub fn get_value_by_key<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    value.get(key)
}

/// Create a Hashmap of all the actions that require to be actioned by the messenger.
/// Key for the map is the Action type. eg: "kafka", "mqtt" ..etc
/// Value for the map contains the payload and some meta information like items actioned, and is completed flag
pub fn get_allowed_commit_actions(on_commit_actions: &Value, allowed_actions: &HashMap<String, Vec<String>>) -> HashMap<String, AllowedActionsMapItem> {
    let mut filtered_actions = HashMap::new();

    allowed_actions.iter().for_each(|(action_key, sub_action_keys)| {
        if let Some(action) = get_value_by_key(on_commit_actions, action_key) {
            for sub_action_key in sub_action_keys {
                if let Some(sub_action) = get_value_by_key(action, sub_action_key) {
                    filtered_actions.insert(
                        sub_action_key.to_string(),
                        AllowedActionsMapItem::new(sub_action.clone(), get_total_action_count(sub_action)),
                    );
                }
            }
        }
    });

    filtered_actions
}

pub fn get_total_action_count(action: &Value) -> u32 {
    if let Some(actions_vec) = action.as_array() {
        actions_vec.len() as u32
    } else {
        1
    }
}

/// Retrieves sub actions under publish by using a look key.
pub fn get_actions_deserialised<T: DeserializeOwned>(actions: &Value) -> Result<T, MessengerActionError> {
    match serde_json::from_value(actions.clone()) {
        Ok(res) => Ok(res),
        Err(err) => Err(MessengerActionError {
            kind: crate::errors::MessengerActionErrorKind::Deserialisation,
            reason: format!("Deserialisation to type={} failed, with error={:?}", type_name::<T>(), err),
            data: actions.to_string(),
        }),
    }
}

///// Retrieves the oncommit actions that are supported by the system.
// fn get_allowed_commit_actions(version: &u64, on_commit_actions: &Value) -> Option<OnCommitActions> {
//     let Some(publish_actions) = on_commit_actions.get("publish") else {
//         warn!("No publish actions found for version={version} in {on_commit_actions}");
//         return None;
//     };

//     match get_sub_actions::<Vec<KafkaAction>>(version, publish_actions, "kafka") {
//         Some(kafka_actions) if !kafka_actions.is_empty() => Some(OnCommitActions::Publish(Some(PublishActions::Kafka(kafka_actions)))),
//         _ => None,
//     }
// }

///// Retrieves sub actions under publish by using a look key.
// fn deserialize_commit_actions<T: DeserializeOwned>(version: &u64, actions: &Value) -> Option<T> {
//     match serde_json::from_value(actions.clone()) {
//         Ok(res) => Some(res),
//         Err(err) => {
//             warn!("Failed to parse  on commit actions for version={version} with error={:?} for {actions}", err);
//             None
//         }
//     }
// }

///// Checks if the relevant oncommit actions are available.
// fn has_supported_commit_actions(version: &u64, on_commit_actions: &Value) -> bool {
//     let Some(publish_actions) = get_value_by_key(on_commit_actions, "publish") else {
//         warn!("No publish actions found for version={version} in {on_commit_actions}");
//         return false;
//     };

//     get_value_by_key(publish_actions, "kafka").is_some()
// }

///// Retrieves sub actions under publish by using a look key.
// fn get_sub_actions<T: DeserializeOwned>(version: &u64, actions: &Value, key: &str) -> Option<T> {
//     let Some(sub_action_value) = actions.get(key) else {
//         warn!("No {key} publish actions found for version={version} in {actions}");
//         return None;
//     };

//     match serde_json::from_value(sub_action_value.clone()) {
//         Ok(res) => Some(res),
//         Err(err) => {
//             warn!(
//                 "Failed to parse {key} on commit actions for version={version} with error={:?} for {actions}",
//                 err
//             );
//             None
//         }
//     }
// }
