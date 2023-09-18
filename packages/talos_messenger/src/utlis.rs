use std::time::Instant;

use log::{error, info, warn};
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::models::commit_actions::publish::{KafkaActions, OnCommitActions, PublishActions};

/// Retrieves the serde_json::Value for a given key
pub fn get_value_by_key<'a>(payload: &'a Value, key: &str) -> Option<&'a Value> {
    payload.get(key)
}

/// Checks if the relevant oncommit actions are available.
pub fn has_supported_commit_actions(version: &u64, on_commit_actions: &Value) -> bool {
    let Some(publish_actions) = get_value_by_key(on_commit_actions, "publish") else {
        warn!("No publish actions found for version={version} in {on_commit_actions}");
        return false;
    };

    get_value_by_key(publish_actions, "kafka").is_some()
}

/// Retrieves the oncommit actions that are supported by the system.
pub fn get_allowed_commit_actions(version: &u64, on_commit_actions: &Value) -> Option<OnCommitActions> {
    let Some(publish_actions) = on_commit_actions.get("publish") else {
        warn!("No publish actions found for version={version} in {on_commit_actions}");
        return None;
    };

    // TODO: GK - In future we will need to check if there are other type that we are interested in, and not just Kafka
    let result = match get_sub_actions::<Vec<KafkaActions>>(version, publish_actions, "kafka") {
        Some(kafka_actions) if !kafka_actions.is_empty() => Some(OnCommitActions::Publish(Some(PublishActions::Kafka(kafka_actions)))),
        _ => None,
    };

    result
}

/// Retrieves sub actions under publish by using a look key.
pub fn deserialize_commit_actions<T: DeserializeOwned>(version: &u64, actions: &Value) -> Option<T> {
    match serde_json::from_value(actions.clone()) {
        Ok(res) => Some(res),
        Err(err) => {
            warn!("Failed to parse  on commit actions for version={version} with error={:?} for {actions}", err);
            None
        }
    }
}
/// Retrieves sub actions under publish by using a look key.
pub fn get_sub_actions<T: DeserializeOwned>(version: &u64, actions: &Value, key: &str) -> Option<T> {
    let Some(sub_action_value) = actions.get(key) else {
        warn!("No {key} publish actions found for version={version} in {actions}");
        return None;
    };

    info!("Sub action we are going to parse is \n {:#?}", sub_action_value);

    match serde_json::from_value(sub_action_value.clone()) {
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
