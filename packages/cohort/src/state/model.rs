use serde::{Deserialize, Serialize};
use serde_json::Value;

use strum::{Display, EnumString};

use std::fmt;
use std::fmt::{Display, Formatter};

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Deserialize)]
pub struct Snapshot {
    pub version: u64,
}

impl Snapshot {
    pub fn is_safe_for(snapshot: Snapshot, safepoint: u64) -> bool {
        snapshot.version >= safepoint
    }
}

impl Display for Snapshot {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Snapshot: [version: {:?}]", self.version)
    }
}

impl From<u64> for Snapshot {
    fn from(value: u64) -> Self {
        Snapshot { version: value }
    }
}

#[derive(Display, Debug, Deserialize, EnumString)]
pub enum BusinessActionType {
    #[strum(serialize = "transfer")]
    TRANSFER,
    DEPOSIT,
    WITHDRAW,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TransferRequest {
    pub from: String,
    pub to: String,
    pub amount: String,
}

impl TransferRequest {
    pub fn new(from: String, to: String, amount: String) -> Self {
        Self { from, to, amount }
    }

    pub fn json(&self) -> Value {
        serde_json::to_value(self).unwrap()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AccountUpdateRequest {
    pub account: String,
    pub amount: String,
}

impl AccountUpdateRequest {
    pub fn new(account: String, amount: String) -> Self {
        Self { account, amount }
    }

    pub fn json(&self) -> Value {
        serde_json::to_value(self).unwrap()
    }
}

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn models() {
        assert_eq!(Snapshot { version: 1 }, Snapshot::from(1));
        assert_ne!(Snapshot { version: 2 }, Snapshot::from(1));

        assert_eq!(format!("{}", Snapshot::from(1)), "Snapshot: [version: 1]".to_string());
    }

    #[test]
    fn should_deserialize_snapshot() {
        let rslt_snapshot = serde_json::from_str::<Snapshot>("{ \"version\": 123456 }");
        let snapshot = rslt_snapshot.unwrap();
        assert_eq!(snapshot.version, 123456);
    }
}
// $coverage:ignore-end
