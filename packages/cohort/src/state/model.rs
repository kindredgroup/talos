use std::fmt;
use std::fmt::{Display, Formatter};

use strum::Display;
use tokio::sync::oneshot::Sender;

use crate::model::bank_account::BankAccount;

pub struct Envelope<T, R> {
    pub data: T,
    pub tx_reply: Sender<R>,
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

#[derive(Display, Clone, Debug, PartialEq)]
pub enum AccountOperation {
    Deposit { amount: String, account: AccountRef },
    Transfer { amount: String, from: AccountRef, to: AccountRef },
    Withdraw { amount: String, account: AccountRef },
    QueryAll,
    QueryAccount { account: AccountRef },
}

#[derive(Display, Clone, Debug, PartialEq)]
pub enum OperationResponse {
    Success,
    Error(String),
    QueryResult(Option<Vec<BankAccount>>),
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

        let _ = format!("{:?}", OperationResponse::Success);
        let _ = format!("{:?}", OperationResponse::Error("e".to_string()));
        let _ = format!("{:?}", OperationResponse::QueryResult(None));
    }
}
// $coverage:ignore-end
