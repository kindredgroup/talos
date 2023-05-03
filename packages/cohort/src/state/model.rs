// $coverage:ignore-start
use serde::{Deserialize, Serialize};
// $coverage:ignore-end
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

#[derive(Display, Debug, Deserialize, EnumString, PartialEq, Eq)]
pub enum BusinessActionType {
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
    use std::str::FromStr;

    use super::*;

    #[test]
    fn models() {
        assert_eq!(Snapshot { version: 1 }, Snapshot::from(1));
        assert_ne!(Snapshot { version: 2 }, Snapshot::from(1));

        assert_eq!(format!("{}", Snapshot::from(1)), "Snapshot: [version: 1]".to_string());
        assert_eq!(
            format!("{:?}", AccountUpdateRequest::new("a1".into(), "10".into())),
            r#"AccountUpdateRequest { account: "a1", amount: "10" }"#.to_string(),
        );
        assert_eq!(
            format!("{:?}", TransferRequest::new("a1".into(), "a2".into(), "10".into())),
            r#"TransferRequest { from: "a1", to: "a2", amount: "10" }"#.to_string(),
        );

        assert_eq!(format!("{:?}", BusinessActionType::DEPOSIT), "DEPOSIT".to_string());
        assert_eq!(format!("{:?}", BusinessActionType::WITHDRAW), "WITHDRAW".to_string());
        assert_eq!(format!("{:?}", BusinessActionType::TRANSFER), "TRANSFER".to_string());

        assert_eq!(BusinessActionType::from_str("TRANSFER").unwrap(), BusinessActionType::TRANSFER);
        assert_eq!(BusinessActionType::from_str("WITHDRAW").unwrap(), BusinessActionType::WITHDRAW);
        assert_eq!(BusinessActionType::from_str("DEPOSIT").unwrap(), BusinessActionType::DEPOSIT);
    }

    #[test]
    fn should_deserialize_snapshot() {
        let rslt_snapshot = serde_json::from_str::<Snapshot>(r#"{ "version": 123456 }"#);
        let snapshot = rslt_snapshot.unwrap();
        assert_eq!(snapshot.version, 123456);
    }

    #[test]
    fn should_deserialize_account_update_request() {
        let rslt = serde_json::from_str::<AccountUpdateRequest>(r#"{ "account": "a1", "amount": "10.0" }"#).unwrap();
        assert_eq!(rslt.account, "a1");
        assert_eq!(rslt.amount, "10.0");
    }

    #[test]
    fn json_for_account_update_request() {
        let json = AccountUpdateRequest::new("a1".into(), "10.0".into()).json();

        assert!(json.get("account").is_some());
        assert_eq!(json.get("account").unwrap(), "a1");

        assert!(json.get("amount").is_some());
        assert_eq!(json.get("amount").unwrap(), "10.0");
    }

    #[test]
    fn should_serialize_account_update_request() {
        let rslt = serde_json::to_string(&AccountUpdateRequest::new("a1".into(), "10.0".into())).unwrap();
        assert_eq!(rslt, r#"{"account":"a1","amount":"10.0"}"#);
    }

    #[test]
    fn should_deserialize_transfer_request() {
        let rslt = serde_json::from_str::<TransferRequest>(r#"{ "from": "a1", "to": "a2", "amount": "10.0" }"#).unwrap();
        assert_eq!(rslt.from, "a1");
        assert_eq!(rslt.to, "a2");
        assert_eq!(rslt.amount, "10.0");
    }

    #[test]
    fn json_for_transfer_request() {
        let json = TransferRequest::new("a1".into(), "a2".into(), "10.0".into()).json();

        assert!(json.get("from").is_some());
        assert_eq!(json.get("from").unwrap(), "a1");

        assert!(json.get("to").is_some());
        assert_eq!(json.get("to").unwrap(), "a2");

        assert!(json.get("amount").is_some());
        assert_eq!(json.get("amount").unwrap(), "10.0");
    }

    #[test]
    fn should_serialize_transfer_request() {
        let rslt = serde_json::to_string(&TransferRequest::new("a1".into(), "a2".into(), "10.0".into())).unwrap();
        assert_eq!(rslt, r#"{"from":"a1","to":"a2","amount":"10.0"}"#);
    }

    #[test]
    #[allow(clippy::bool_assert_comparison)]
    fn when_snapshot_is_behind_safepoint_ist_not_safe() {
        let safepoint = 2;
        assert_eq!(Snapshot::is_safe_for(Snapshot { version: 1 }, safepoint), false);
        assert_eq!(Snapshot::is_safe_for(Snapshot { version: 2 }, safepoint), true);
        assert_eq!(Snapshot::is_safe_for(Snapshot { version: 3 }, safepoint), true);
    }
}
// $coverage:ignore-end
