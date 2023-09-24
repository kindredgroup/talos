use ahash::{HashMap, HashMapExt};
use log::warn;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::suffix::AllowedActionsMapItem;

/// Retrieves the serde_json::Value for a given key
pub fn get_value_by_key<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    value.get(key)
}

/// Create a Hashmap of all the actions that require to be actioned by the messenger.
/// Key for the map is the Action type. eg: "kafka", "mqtt" ..etc
/// Value for the map contains the payload and some meta information like items actioned, and is completed flag
pub fn get_allowed_commit_actions(
    on_commit_actions: &Value,
    allowed_actions: &HashMap<&'static str, Vec<&'static str>>,
) -> HashMap<String, AllowedActionsMapItem> {
    let mut filtered_actions = HashMap::new();

    allowed_actions.iter().for_each(|(action_key, sub_action_keys)| {
        if let Some(action) = get_value_by_key(on_commit_actions, action_key) {
            for sub_action_key in sub_action_keys {
                if let Some(sub_action) = get_value_by_key(action, sub_action_key) {
                    filtered_actions.insert(
                        sub_action_key.to_string(),
                        AllowedActionsMapItem {
                            payload: sub_action.clone(),
                            count: 0,
                            is_completed: false,
                        },
                    );
                }
            }
        }
    });

    filtered_actions
}

/// Retrieves sub actions under publish by using a look key.
pub fn get_actions_deserialised<T: DeserializeOwned>(version: &u64, actions: &Value, key: &str) -> Option<T> {
    match serde_json::from_value(actions.clone()) {
        Ok(res) => Some(res),
        Err(err) => {
            warn!(
                "Failed to parse {key} on commit actions for version={version} with error={:?} for {actions}",
                err
            );
            None
        }
    }
}

///// Retrieves the oncommit actions that are supported by the system.
// fn get_allowed_commit_actions(version: &u64, on_commit_actions: &Value) -> Option<OnCommitActions> {
//     let Some(publish_actions) = on_commit_actions.get("publish") else {
//         warn!("No publish actions found for version={version} in {on_commit_actions}");
//         return None;
//     };

//     // TODO: GK - In future we will need to check if there are other type that we are interested in, and not just Kafka
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
