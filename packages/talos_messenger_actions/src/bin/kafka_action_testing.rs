// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// TODO: GK - This is a temporary testing bin, should be deleted before PR is merged.
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

use log::info;
use talos_messenger_actions::kafka::models::KafkaAction;
// use serde_json;
use talos_messenger_core::utlis::{get_sub_actions, get_value_by_key};

use serde::{Deserialize, Serialize}; // 1.0.130
use serde_json::{self};
use strum::{Display, EnumString}; // 1.0.67

#[derive(Debug, Display, Serialize, Deserialize, EnumString, Clone, Eq, PartialEq)]
pub enum PublishActions {
    #[serde(rename(deserialize = "kafka"))]
    // #[strum(default)]
    Kafka(Vec<KafkaAction>),
}

#[derive(Debug, Display, EnumString, Serialize, Deserialize, Clone, Eq, PartialEq)]
enum OnCommitActions {
    #[serde(rename(deserialize = "publish"))]
    Publish(Option<PublishActions>),
}

fn main() {
    env_logger::init();

    let input = serde_json::json!({
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
            "kaf2ka": [
                {
                    "_typ": "KafkaMessage",
                    "topic": "${env}.chimera.coupons", // ${env} is substituted by the Kafka provider
                    "value": "test"
                }
            ]
        }
    });
    // let data = get_allowed_commit_actions(&10, &input);
    let publish_action = get_value_by_key(&input, "publish").unwrap();

    let data = get_sub_actions::<Vec<KafkaAction>>(&10, publish_action, "kafka");
    info!("{:#?}", data);
}
