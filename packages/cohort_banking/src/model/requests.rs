use std::collections::HashMap;

// $coverage:ignore-start
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
// $coverage:ignore-end
use serde_json::Value;

use strum::{Display, EnumString};

#[derive(Clone)]
pub struct CandidateData {
    pub readset: Vec<String>,
    pub writeset: Vec<String>,
    pub statemap: Option<Vec<HashMap<String, Value>>>,
    // The "snapshot" is intentionally messing here. We will compute it ourselves before feeding this data to Talos
}

#[derive(Clone)]
pub struct CertificationRequest {
    pub candidate: CandidateData,
    pub timeout_ms: u64,
}

#[derive(Display, Debug, Deserialize, EnumString, PartialEq, Eq)]
pub enum BusinessActionType {
    TRANSFER,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TransferRequest {
    pub from: String,
    pub to: String,
    #[serde(with = "rust_decimal::serde::float")]
    pub amount: Decimal,
}

impl TransferRequest {
    pub fn new(from: String, to: String, amount: Decimal) -> Self {
        Self { from, to, amount }
    }

    pub fn json(&self) -> Value {
        serde_json::to_value(self).unwrap()
    }
}

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::{prelude::FromPrimitive, Decimal};
    use std::str::FromStr;

    #[test]
    fn models() {
        assert_eq!(
            format!("{:?}", TransferRequest::new("a1".into(), "a2".into(), Decimal::from_f32(10.0).unwrap())),
            r#"TransferRequest { from: "a1", to: "a2", amount: 10 }"#.to_string(),
        );

        assert_eq!(format!("{:?}", BusinessActionType::TRANSFER), "TRANSFER".to_string());
        assert_eq!(BusinessActionType::from_str("TRANSFER").unwrap(), BusinessActionType::TRANSFER);
    }

    #[test]
    fn should_deserialize_transfer_request() {
        let rslt = serde_json::from_str::<TransferRequest>(r#"{ "from": "a1", "to": "a2", "amount": 10.0 }"#).unwrap();
        assert_eq!(rslt.from, "a1");
        assert_eq!(rslt.to, "a2");
        assert_eq!(rslt.amount, Decimal::from_f32(10.0).unwrap());
    }

    #[test]
    fn json_for_transfer_request() {
        let json = TransferRequest::new("a1".into(), "a2".into(), Decimal::from_f32(10.0).unwrap()).json();

        assert!(json.get("from").is_some());
        assert_eq!(json.get("from").unwrap(), "a1");

        assert!(json.get("to").is_some());
        assert_eq!(json.get("to").unwrap(), "a2");

        assert!(json.get("amount").is_some());
        assert_eq!(json.get("amount").unwrap(), 10.0);
    }

    #[test]
    fn should_serialize_transfer_request() {
        let rslt = serde_json::to_string(&TransferRequest::new("a1".into(), "a2".into(), Decimal::from_f32(10.0).unwrap())).unwrap();
        assert_eq!(rslt, r#"{"from":"a1","to":"a2","amount":10.0}"#);
    }
}
// $coverage:ignore-end
