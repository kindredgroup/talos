// $coverage:ignore-start
use crate::model::bank_account::BankAccount;
// $coverage:ignore-end

use serde::Deserialize;
use std::fmt;
use std::fmt::{Display, Formatter};

use strum::Display;
use tokio::sync::oneshot::Sender;

pub struct Envelope<T, R> {
    pub data: T,
    pub tx_reply: Sender<R>,
}

impl<T, R> Envelope<T, R> {
    pub fn new(data: T, tx_reply: Sender<R>) -> Self {
        Self { data, tx_reply }
    }
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub struct AccountRef {
    pub number: String,
    pub new_version: Option<u64>,
}

impl Display for AccountRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "AccountRef: [number: {}, new_version: {:?}]", self.number, self.new_version)
    }
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Deserialize)]
pub struct Snapshot {
    pub version: u64,
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

#[derive(Display, Clone, Debug, PartialEq)]
pub enum AccountOperation {
    Deposit { amount: String, account: AccountRef },
    Transfer { amount: String, from: AccountRef, to: AccountRef },
    Withdraw { amount: String, account: AccountRef },
    QueryAll,
    QueryAccount { account: AccountRef },
    QuerySnapshot,
    UpdateSnapshot(u64),
}

#[derive(Display, Clone, Debug, PartialEq)]
pub enum OperationResponse {
    Success,
    Error(String),
    QueryResult(Option<Vec<BankAccount>>),
    Snapshot(Snapshot),
}

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn models() {
        let acc_ref = AccountRef {
            number: "1".to_string(),
            new_version: None,
        };
        assert_eq!(
            acc_ref,
            AccountRef {
                number: "1".to_string(),
                new_version: None,
            },
        );
        assert_ne!(
            acc_ref,
            AccountRef {
                number: "1".to_string(),
                new_version: Some(1),
            },
        );
        assert_eq!(format!("{}", acc_ref), "AccountRef: [number: 1, new_version: None]".to_string());

        assert_eq!(Snapshot { version: 1 }, Snapshot::from(1));
        assert_ne!(Snapshot { version: 2 }, Snapshot::from(1));

        assert_eq!(format!("{}", Snapshot::from(1)), "Snapshot: [version: 1]".to_string());

        let _ = format!("{:?}", AccountOperation::QueryAccount { account: acc_ref.clone() });
        let _ = format!("{:?}", AccountOperation::QueryAll);
        let _ = format!(
            "{:?}",
            AccountOperation::Withdraw {
                amount: "1".to_string(),
                account: acc_ref.clone(),
            }
        );
        let _ = format!(
            "{:?}",
            AccountOperation::Transfer {
                amount: "1".to_string(),
                from: acc_ref.clone(),
                to: acc_ref.clone(),
            }
        );
        let _ = format!(
            "{:?}",
            AccountOperation::Deposit {
                amount: "1".to_string(),
                account: acc_ref,
            },
        );
        let _ = format!("{:?}", AccountOperation::QuerySnapshot);
        let _ = format!("{:?}", AccountOperation::UpdateSnapshot(1));

        let _ = format!("{:?}", OperationResponse::Success);
        let _ = format!("{:?}", OperationResponse::Error("e".to_string()));
        let _ = format!("{:?}", OperationResponse::QueryResult(None));
        let _ = format!("{:?}", OperationResponse::Snapshot(Snapshot::from(1)));
    }

    #[test]
    fn should_deserialize_snapshot() {
        let rslt_snapshot = serde_json::from_str::<Snapshot>("{ \"version\": 123456 }");
        let snapshot = rslt_snapshot.unwrap();
        assert_eq!(snapshot.version, 123456);
    }
}
// $coverage:ignore-end
