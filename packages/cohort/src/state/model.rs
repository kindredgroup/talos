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

#[derive(Display, Clone, Debug)]
pub enum AccountOperation {
    Deposit { amount: String, account: AccountRef },
    Transfer { amount: String, from: AccountRef, to: AccountRef },
    Withdraw { amount: String, account: AccountRef },
    QueryAll,
    QueryAccount { account: AccountRef },
}

#[derive(Display, Clone, Debug)]
pub enum OperationResponse {
    Success,
    Error(String),
    QueryResult(Option<Vec<BankAccount>>),
}
