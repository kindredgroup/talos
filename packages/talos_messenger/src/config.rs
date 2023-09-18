use ahash::{HashMap, HashMapExt};
use serde::{Deserialize, Serialize};

type WhitelistSubActions = String;

// pub const whitelist_actions: Vec<WhitelistActions> = get_whitelisted_actions();

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct WhitelistActions {
    pub action: String,
    #[serde(rename(deserialize = "subActions"), default)]
    pub sub_actions: Vec<WhitelistSubActions>,
}

pub fn get_whitelisted_actions() -> Vec<WhitelistActions> {
    let actions_allowed = serde_json::json!([ {
                "action":"publish",
                "subActions": ["kafka"]
            }
        ]
    );

    let whitelist_actions: Vec<WhitelistActions> = serde_json::from_value(actions_allowed).unwrap();

    whitelist_actions
}
