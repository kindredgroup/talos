use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase", tag = "_typ")]
pub struct DeliveryOrder {
    // CONTROL FIELDS
    pub topic: String,
    // DATA FIELDS
    pub headers: HashMap<String, String>,
    pub key: String,
    pub value: String,
}
