use rusty_money::iso::Currency;
use rusty_money::Money;
use tokio::sync::mpsc::Sender;

use crate::model::bank_account::BankAccount;
use crate::state::model::{AccountOperation, AccountRef, Envelope, OperationResponse};

pub struct Bank {}

impl Bank {
    pub async fn get_accounts(tx_state: Sender<Envelope<AccountOperation, OperationResponse>>) -> Result<Vec<BankAccount>, String> {
        let (tx, rx) = tokio::sync::oneshot::channel::<OperationResponse>();
        let resp = tx_state
            .send(Envelope {
                data: AccountOperation::QueryAll,
                tx_reply: tx,
            })
            .await;
        if let Err(se) = resp {
            return Err(format!("Internal error requesting bank operation: {}", se));
        }

        let rslt = rx.await;
        if let Err(re) = rslt {
            return Err(format!("Internal error waiting for answer: {}", re));
        }

        match rslt.unwrap() {
            OperationResponse::Success => Err("Unexpected response 'Success' to QueryAll".to_string()),
            OperationResponse::Error(e) => Err(format!("Error querying bank accounts; {}", e)),
            OperationResponse::QueryResult(None) => Err("No bank accounts found".to_string()),
            OperationResponse::QueryResult(Some(accounts_list)) => Ok(accounts_list),
        }
    }

    pub async fn get_balance(tx_state: Sender<Envelope<AccountOperation, OperationResponse>>, account: AccountRef) -> Result<Money<'static, Currency>, String> {
        let (tx, rx) = tokio::sync::oneshot::channel::<OperationResponse>();
        let resp = tx_state
            .send(Envelope {
                data: AccountOperation::QueryAccount { account },
                tx_reply: tx,
            })
            .await;
        if let Err(se) = resp {
            return Err(format!("Internal error requesting bank operation: {}", se));
        }

        let rslt = rx.await;
        if let Err(re) = rslt {
            return Err(format!("Internal error waiting for answer: {}", re));
        }

        match rslt.unwrap() {
            OperationResponse::Success => Err("Unexpected response 'Success' to QueryAccount".to_string()),
            OperationResponse::Error(e) => Err(format!("Error querying bank account: {}", e)),
            OperationResponse::QueryResult(None) => Err("Bank account is not found".to_string()),
            OperationResponse::QueryResult(Some(accounts_list)) => Ok(accounts_list.get(0).unwrap().balance.clone()),
        }
    }

    pub async fn deposit(tx_state: Sender<Envelope<AccountOperation, OperationResponse>>, account: &BankAccount, amount: String) -> Result<(), String> {
        let (tx, rx) = tokio::sync::oneshot::channel::<OperationResponse>();
        let resp = tx_state
            .send(Envelope {
                data: AccountOperation::Deposit {
                    account: AccountRef {
                        number: account.number.clone(),
                        new_version: None,
                    },
                    amount,
                },
                tx_reply: tx,
            })
            .await;

        if let Err(se) = resp {
            return Err(format!("Internal error requesting bank operation: {}", se));
        }
        let rslt = rx.await;
        if let Err(re) = rslt {
            return Err(format!("Internal error waiting for answer: {}", re));
        }

        match rslt.unwrap() {
            OperationResponse::Success => Ok(()),
            OperationResponse::Error(e) => Err(format!("Error depositing to bank account: {}", e)),
            OperationResponse::QueryResult(_) => Err("Unexpected response 'QueryResult' to Deposit".to_string()),
        }
    }

    pub async fn withdraw(tx_state: Sender<Envelope<AccountOperation, OperationResponse>>, account: &BankAccount, amount: String) -> Result<(), String> {
        let (tx, rx) = tokio::sync::oneshot::channel::<OperationResponse>();
        let resp = tx_state
            .send(Envelope {
                data: AccountOperation::Withdraw {
                    account: AccountRef {
                        number: account.number.clone(),
                        new_version: None,
                    },
                    amount,
                },
                tx_reply: tx,
            })
            .await;

        if let Err(se) = resp {
            return Err(format!("Internal error requesting bank operation: {}", se));
        }
        let rslt = rx.await;
        if let Err(re) = rslt {
            return Err(format!("Internal error waiting for answer: {}", re));
        }

        match rslt.unwrap() {
            OperationResponse::Success => Ok(()),
            OperationResponse::Error(e) => Err(format!("Error withdrawing from bank account: {}", e)),
            OperationResponse::QueryResult(_) => Err("Unexpected response 'QueryResult' to Withdraw".to_string()),
        }
    }

    pub async fn transfer(
        tx_state: Sender<Envelope<AccountOperation, OperationResponse>>,
        from: &BankAccount,
        to: &BankAccount,
        amount: String,
    ) -> Result<(), String> {
        let (tx, rx) = tokio::sync::oneshot::channel::<OperationResponse>();
        let resp = tx_state
            .send(Envelope {
                data: AccountOperation::Transfer {
                    amount,
                    from: AccountRef {
                        number: from.number.clone(),
                        new_version: None,
                    },
                    to: AccountRef {
                        number: to.number.clone(),
                        new_version: None,
                    },
                },
                tx_reply: tx,
            })
            .await;

        if let Err(se) = resp {
            return Err(format!("Internal error requesting bank operation: {}", se));
        }
        let rslt = rx.await;
        if let Err(re) = rslt {
            return Err(format!("Internal error waiting for answer: {}", re));
        }

        match rslt.unwrap() {
            OperationResponse::Success => Ok(()),
            OperationResponse::Error(e) => Err(format!("Error transferring between bank accounts: {}", e)),
            OperationResponse::QueryResult(_) => Err("Unexpected response 'QueryResult' to Transfer".to_string()),
        }
    }
}
