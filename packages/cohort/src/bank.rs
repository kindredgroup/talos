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

    pub async fn deposit(tx_state: Sender<Envelope<AccountOperation, OperationResponse>>, account: AccountRef, amount: String) -> Result<(), String> {
        let (tx, rx) = tokio::sync::oneshot::channel::<OperationResponse>();
        let resp = tx_state
            .send(Envelope {
                data: AccountOperation::Deposit { account, amount },
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

    pub async fn withdraw(tx_state: Sender<Envelope<AccountOperation, OperationResponse>>, account: AccountRef, amount: String) -> Result<(), String> {
        let (tx, rx) = tokio::sync::oneshot::channel::<OperationResponse>();
        let resp = tx_state
            .send(Envelope {
                data: AccountOperation::Withdraw { account, amount },
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
        from: AccountRef,
        to: AccountRef,
        amount: String,
    ) -> Result<(), String> {
        let (tx, rx) = tokio::sync::oneshot::channel::<OperationResponse>();
        let resp = tx_state
            .send(Envelope {
                data: AccountOperation::Transfer { amount, from, to },
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

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::talos_state::TalosState;
    use tokio::task::JoinHandle;

    fn get_test_accounts() -> Vec<BankAccount> {
        vec![
            BankAccount::aud("a1".to_string(), "a11".to_string(), "101".to_string(), TalosState { version: 1 }),
            BankAccount::aud("a2".to_string(), "a12".to_string(), "102".to_string(), TalosState { version: 1 }),
            BankAccount::aud("a3".to_string(), "a13".to_string(), "103".to_string(), TalosState { version: 1 }),
        ]
    }

    async fn mock_state_manager<F>(
        expected_receive: AccountOperation,
        fn_replier: F,
    ) -> (Sender<Envelope<AccountOperation, OperationResponse>>, JoinHandle<String>)
    where
        F: Fn(tokio::sync::oneshot::Sender<OperationResponse>) -> String + Send + Sync + 'static,
    {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Envelope<AccountOperation, OperationResponse>>(10);
        let handle = tokio::task::spawn(async move {
            let data = rx.recv().await;
            if data.is_none() {
                return "No data received".to_string();
            }
            let envelope = data.unwrap();
            if envelope.data != expected_receive {
                return format!("Invalid operation {:?}, ${:?} is expected", envelope.data, expected_receive);
            }

            fn_replier(envelope.tx_reply)
        });

        (tx, handle)
    }

    fn mock_with_closed_receiver() -> Sender<Envelope<AccountOperation, OperationResponse>> {
        let (tx, rx) = tokio::sync::mpsc::channel::<Envelope<AccountOperation, OperationResponse>>(10);
        drop(rx);
        tx
    }

    async fn mock_response_success(operation: &AccountOperation) -> (Sender<Envelope<AccountOperation, OperationResponse>>, JoinHandle<String>) {
        let (tx, handle) = mock_state_manager(operation.clone(), |tx| {
            let _ = tx.send(OperationResponse::Success);
            "Success".to_string()
        })
        .await;
        (tx, handle)
    }

    async fn mock_response_empty_query(operation: &AccountOperation) -> (Sender<Envelope<AccountOperation, OperationResponse>>, JoinHandle<String>) {
        let (tx, handle) = mock_state_manager(operation.clone(), |tx| {
            let _ = tx.send(OperationResponse::QueryResult(None));
            "Success".to_string()
        })
        .await;
        (tx, handle)
    }

    async fn mock_resp_wrong_account(operation: &AccountOperation) -> (Sender<Envelope<AccountOperation, OperationResponse>>, JoinHandle<String>) {
        let (tx, handle) = mock_state_manager(operation.clone(), |tx| {
            let _ = tx.send(OperationResponse::Error("wrong account".to_string()));
            "Success".to_string()
        })
        .await;
        (tx, handle)
    }

    async fn mock_with_closed_reply_channel(operation: &AccountOperation) -> (Sender<Envelope<AccountOperation, OperationResponse>>, JoinHandle<String>) {
        let (tx, handle) = mock_state_manager(operation.clone(), |tx| {
            drop(tx);
            "Success".to_string()
        })
        .await;
        (tx, handle)
    }

    fn expect_error<T>(response: Result<T, String>, error: &str) {
        let expected_error = if let Err(e) = response {
            if e.contains(error) {
                true
            } else {
                log::info!("Expected: '{}', got: '{}'", error, e);
                false
            }
        } else {
            false
        };
        assert!(expected_error);
    }

    #[tokio::test]
    async fn get_accounts() {
        // Simulate state manager...
        let (tx, handle) = mock_state_manager(AccountOperation::QueryAll, |tx| {
            let _ = tx.send(OperationResponse::QueryResult(Some(get_test_accounts())));
            "Success".to_string()
        })
        .await;

        let resp = Bank::get_accounts(tx).await;
        assert!(resp.is_ok());
        assert_eq!("Success", handle.await.unwrap());
    }

    #[tokio::test]
    async fn get_accounts_should_fail() {
        env_logger::builder().format_timestamp_millis().init();

        let operation = AccountOperation::QueryAll;
        // Simulate internal channels are closed
        let (tx, handle) = mock_with_closed_reply_channel(&operation).await;
        let resp = Bank::get_accounts(tx).await;
        expect_error(resp, "channel closed");
        assert_eq!("Success", handle.await.unwrap());

        let resp = Bank::get_accounts(mock_with_closed_receiver()).await;
        expect_error(resp, "Internal error requesting bank operation: channel closed");

        let (tx, handle) = mock_state_manager(AccountOperation::QueryAll, |tx| {
            let _ = tx.send(OperationResponse::QueryResult(None));
            "Success".to_string()
        })
        .await;

        let resp = Bank::get_accounts(tx).await;
        expect_error(resp, "No bank accounts found");
        assert_eq!("Success", handle.await.unwrap());

        let (tx, handle) = mock_state_manager(AccountOperation::QueryAll, |tx| {
            let _ = tx.send(OperationResponse::Error("some-test-error".to_string()));
            "Success".to_string()
        })
        .await;

        let resp = Bank::get_accounts(tx).await;
        expect_error(resp, "some-test-error");
        assert_eq!("Success", handle.await.unwrap());

        let (tx, handle) = mock_state_manager(AccountOperation::QueryAll, |tx| {
            let _ = tx.send(OperationResponse::Success);
            "Success".to_string()
        })
        .await;

        let resp = Bank::get_accounts(tx).await;
        expect_error(resp, "Unexpected response 'Success'");
        assert_eq!("Success", handle.await.unwrap());
    }

    #[tokio::test]
    async fn get_balance() {
        // Simulate state manager...
        let a12 = AccountRef {
            number: "a12".to_string(),
            new_version: None,
        };
        let (tx, handle) = mock_state_manager(AccountOperation::QueryAccount { account: a12.clone() }, |tx| {
            let _ = tx.send(OperationResponse::QueryResult(Some(vec![get_test_accounts().get(1).unwrap().clone()])));
            "Success".to_string()
        })
        .await;

        let resp = Bank::get_balance(tx, a12.clone()).await;
        assert!(resp.is_ok());

        let balance = resp.unwrap();
        assert_eq!(balance.amount().to_string(), "102.00");
        assert_eq!(balance.currency().iso_alpha_code, "AUD");
        assert_eq!("Success", handle.await.unwrap());
    }

    #[tokio::test]
    async fn get_balance_should_fail() {
        // Simulate state manager replying bad account...
        let a12 = AccountRef {
            number: "a12".to_string(),
            new_version: None,
        };
        let operation = AccountOperation::QueryAccount { account: a12.clone() };

        let resp = Bank::get_balance(mock_with_closed_receiver(), a12.clone()).await;
        expect_error(resp, "Internal error requesting bank operation: channel closed");

        let (tx, handle) = mock_with_closed_reply_channel(&operation).await;
        let resp = Bank::get_balance(tx, a12.clone()).await;
        expect_error(resp, "channel closed");
        assert_eq!("Success", handle.await.unwrap());

        let (tx, handle) = mock_resp_wrong_account(&operation).await;
        let resp = Bank::get_balance(tx, a12.clone()).await;
        expect_error(resp, "wrong account");
        assert_eq!("Success", handle.await.unwrap());

        // Simulate state manager replying None...
        let (tx, handle) = mock_response_empty_query(&operation).await;
        let resp = Bank::get_balance(tx, a12.clone()).await;
        expect_error(resp, "Bank account is not found");
        assert_eq!("Success", handle.await.unwrap());

        // Simulate state manager unexpectedly replying Success...
        let (tx, handle) = mock_response_success(&operation).await;
        let resp = Bank::get_balance(tx, a12.clone()).await;
        expect_error(resp, "Unexpected response 'Success'");
        assert_eq!("Success", handle.await.unwrap());
    }

    #[tokio::test]
    async fn withdraw() {
        let a12 = AccountRef {
            number: "a12".to_string(),
            new_version: None,
        };
        let operation = AccountOperation::Withdraw {
            amount: "10".to_string(),
            account: a12.clone(),
        };

        let (tx, handle) = mock_state_manager(operation, |tx| {
            let _ = tx.send(OperationResponse::Success);
            "Success".to_string()
        })
        .await;
        let resp = Bank::withdraw(tx, a12.clone(), "10".to_string()).await;
        assert!(resp.is_ok());
        assert_eq!("Success", handle.await.unwrap());
    }

    #[tokio::test]
    async fn withdraw_should_fail() {
        // Simulate state manager replying bad account...
        let a12 = AccountRef {
            number: "a12".to_string(),
            new_version: None,
        };
        let operation = AccountOperation::Withdraw {
            amount: "10".to_string(),
            account: a12.clone(),
        };

        let resp = Bank::withdraw(mock_with_closed_receiver(), a12.clone(), "10".to_string()).await;
        expect_error(resp, "Internal error requesting bank operation: channel closed");

        let (tx, handle) = mock_with_closed_reply_channel(&operation).await;
        let resp = Bank::withdraw(tx, a12.clone(), "10".to_string()).await;
        expect_error(resp, "channel closed");
        assert_eq!("Success", handle.await.unwrap());

        let (tx, handle) = mock_resp_wrong_account(&operation).await;
        let resp = Bank::withdraw(tx, a12.clone(), "10".to_string()).await;
        expect_error(resp, "wrong account");
        assert_eq!("Success", handle.await.unwrap());

        let (tx, handle) = mock_response_empty_query(&operation).await;
        let resp = Bank::withdraw(tx, a12.clone(), "10".to_string()).await;
        expect_error(resp, "Unexpected response 'QueryResult'");
        assert_eq!("Success", handle.await.unwrap());

        let (tx, handle) = mock_response_success(&operation).await;
        let resp = Bank::withdraw(tx, a12.clone(), "10".to_string()).await;
        assert!(resp.is_ok());
        assert_eq!("Success", handle.await.unwrap());
    }

    #[tokio::test]
    async fn deposit() {
        let a12 = AccountRef {
            number: "a12".to_string(),
            new_version: None,
        };
        let operation = AccountOperation::Deposit {
            amount: "10".to_string(),
            account: a12.clone(),
        };

        let (tx, handle) = mock_state_manager(operation, |tx| {
            let _ = tx.send(OperationResponse::Success);
            "Success".to_string()
        })
        .await;
        let resp = Bank::deposit(tx, a12.clone(), "10".to_string()).await;
        assert!(resp.is_ok());
        assert_eq!("Success", handle.await.unwrap());
    }

    #[tokio::test]
    async fn deposit_should_fail() {
        // Simulate state manager replying bad account...
        let a12 = AccountRef {
            number: "a12".to_string(),
            new_version: None,
        };
        let operation = AccountOperation::Deposit {
            amount: "10".to_string(),
            account: a12.clone(),
        };

        let resp = Bank::deposit(mock_with_closed_receiver(), a12.clone(), "10".to_string()).await;
        expect_error(resp, "Internal error requesting bank operation: channel closed");

        let (tx, handle) = mock_with_closed_reply_channel(&operation).await;
        let resp = Bank::deposit(tx, a12.clone(), "10".to_string()).await;
        expect_error(resp, "channel closed");
        assert_eq!("Success", handle.await.unwrap());

        let (tx, handle) = mock_resp_wrong_account(&operation).await;
        let resp = Bank::deposit(tx, a12.clone(), "10".to_string()).await;
        expect_error(resp, "wrong account");
        assert_eq!("Success", handle.await.unwrap());

        let (tx, handle) = mock_response_empty_query(&operation).await;
        let resp = Bank::deposit(tx, a12.clone(), "10".to_string()).await;
        expect_error(resp, "Unexpected response 'QueryResult'");
        assert_eq!("Success", handle.await.unwrap());

        let (tx, handle) = mock_response_success(&operation).await;
        let resp = Bank::deposit(tx, a12.clone(), "10".to_string()).await;
        assert!(resp.is_ok());
        assert_eq!("Success", handle.await.unwrap());
    }

    #[tokio::test]
    async fn transfer() {
        let a12 = AccountRef {
            number: "a12".to_string(),
            new_version: None,
        };
        let a13 = AccountRef {
            number: "a13".to_string(),
            new_version: None,
        };
        let operation = AccountOperation::Transfer {
            amount: "10".to_string(),
            from: a12.clone(),
            to: a13.clone(),
        };

        let (tx, handle) = mock_state_manager(operation, |tx| {
            let _ = tx.send(OperationResponse::Success);
            "Success".to_string()
        })
        .await;
        let resp = Bank::transfer(tx, a12.clone(), a13.clone(), "10".to_string()).await;
        assert!(resp.is_ok());
        assert_eq!("Success", handle.await.unwrap());
    }

    #[tokio::test]
    async fn transfer_should_fail() {
        // Simulate state manager replying bad account...
        let a12 = AccountRef {
            number: "a12".to_string(),
            new_version: None,
        };
        let a13 = AccountRef {
            number: "a13".to_string(),
            new_version: None,
        };
        let operation = AccountOperation::Transfer {
            amount: "10".to_string(),
            from: a12.clone(),
            to: a13.clone(),
        };

        let resp = Bank::transfer(mock_with_closed_receiver(), a12.clone(), a13.clone(), "10".to_string()).await;
        expect_error(resp, "Internal error requesting bank operation: channel closed");

        let (tx, handle) = mock_with_closed_reply_channel(&operation).await;
        let resp = Bank::transfer(tx, a12.clone(), a13.clone(), "10".to_string()).await;
        expect_error(resp, "channel closed");
        assert_eq!("Success", handle.await.unwrap());

        let (tx, handle) = mock_resp_wrong_account(&operation).await;
        let resp = Bank::transfer(tx, a12.clone(), a13.clone(), "10".to_string()).await;
        expect_error(resp, "wrong account");
        assert_eq!("Success", handle.await.unwrap());

        let (tx, handle) = mock_response_empty_query(&operation).await;
        let resp = Bank::transfer(tx, a12.clone(), a13.clone(), "10".to_string()).await;
        expect_error(resp, "Unexpected response 'QueryResult'");
        assert_eq!("Success", handle.await.unwrap());

        let (tx, handle) = mock_response_success(&operation).await;
        let resp = Bank::transfer(tx, a12.clone(), a13.clone(), "10".to_string()).await;
        assert!(resp.is_ok());
        assert_eq!("Success", handle.await.unwrap());
    }
}
// $coverage:ignore-end
