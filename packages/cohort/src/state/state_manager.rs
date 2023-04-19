use std::sync::Arc;

use rusty_money::iso;
use tokio::sync::mpsc::Receiver;

use crate::model::bank_account::as_money;
use crate::state::model::{AccountOperation, Envelope, OperationResponse};
use crate::state::postgres::data_store::DataStore;
use crate::state::postgres::database::Database;

pub struct StateManager {
    pub database: Arc<Database>,
}

impl StateManager {
    // $coverage:ignore-start
    pub async fn run(&mut self, mut rx_api: Receiver<Envelope<AccountOperation, OperationResponse>>) {
        loop {
            tokio::select! {
                rslt_request_msg = rx_api.recv() => {
                    if rslt_request_msg.is_some() {
                        self.handle(rslt_request_msg.unwrap()).await;
                    }
                }
            }
        }
    }
    // $coverage:ignore-end

    async fn handle(&mut self, request: Envelope<AccountOperation, OperationResponse>) {
        let answer = match request.data {
            AccountOperation::QueryAll => {
                let accounts = DataStore::get_accounts(Arc::clone(&self.database)).await.unwrap();
                OperationResponse::QueryResult(Some(accounts))
            }

            AccountOperation::QueryAccount { account: account_ref } => {
                let account = DataStore::get_account(Arc::clone(&self.database), account_ref.number).await.unwrap();
                if account.is_none() {
                    OperationResponse::QueryResult(None)
                } else {
                    OperationResponse::QueryResult(Some(vec![account.unwrap()]))
                }
            }

            AccountOperation::Deposit { amount, account: account_ref } => match as_money(amount, iso::find("AUD").unwrap()) {
                Ok(amount) => {
                    if let Err(e) = DataStore::deposit(Arc::clone(&self.database), &amount.amount().to_string(), account_ref).await {
                        OperationResponse::Error(e)
                    } else {
                        OperationResponse::Success
                    }
                }
                Err(e) => OperationResponse::Error(e),
            },

            AccountOperation::Withdraw { amount, account: account_ref } => match as_money(amount, iso::find("AUD").unwrap()) {
                Ok(amount) => {
                    if let Err(e) = DataStore::withdraw(Arc::clone(&self.database), &amount.amount().to_string(), account_ref).await {
                        OperationResponse::Error(e)
                    } else {
                        OperationResponse::Success
                    }
                }
                Err(e) => OperationResponse::Error(e),
            },

            AccountOperation::Transfer { amount, from, to } => {
                if let Err(e) = DataStore::transfer(Arc::clone(&self.database), &amount, from.clone(), to.clone()).await {
                    OperationResponse::Error(format!("Unable to transfer {} from {} to {}. Error: {}", amount, from, to, e))
                } else {
                    OperationResponse::Success
                }
            }

            AccountOperation::QuerySnapshot => match DataStore::get_snapshot(Arc::clone(&self.database)).await {
                Ok(snapshot) => OperationResponse::Snapshot(snapshot),
                Err(e) => OperationResponse::Error(e),
            },

            AccountOperation::UpdateSnapshot(new_version) => {
                if let Err(e) = DataStore::update_snapshot(Arc::clone(&self.database), new_version).await {
                    OperationResponse::Error(e)
                } else {
                    OperationResponse::Success
                }
            }
        };

        request.tx_reply.send(answer).unwrap();
    }
}

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use crate::model::bank_account::BankAccount;
    use crate::model::talos_state::TalosState;
    use crate::state::model::AccountRef;

    use super::*;

    fn get_bad_account() -> AccountRef {
        AccountRef {
            number: "_".to_string(),
            new_version: None,
        }
    }

    fn get_test_accounts() -> Vec<BankAccount> {
        vec![
            BankAccount::aud("a1".to_string(), "a11".to_string(), "101".to_string(), TalosState { version: 1 }),
            BankAccount::aud("a2".to_string(), "a12".to_string(), "102".to_string(), TalosState { version: 1 }),
            BankAccount::aud("a3".to_string(), "a13".to_string(), "103".to_string(), TalosState { version: 1 }),
        ]
    }

    fn get_fixture() -> StateManager {
        StateManager {}
    }

    async fn send(state_manager: &mut StateManager, req: AccountOperation) -> OperationResponse {
        let (tx, rx) = tokio::sync::oneshot::channel::<OperationResponse>();
        let req = Envelope::new(req, tx);
        state_manager.handle(req).await;
        rx.await.unwrap()
    }

    async fn transfer(state_manager: &mut StateManager, amount: &str, from: AccountRef, to: AccountRef) -> OperationResponse {
        send(
            state_manager,
            AccountOperation::Transfer {
                amount: amount.to_string(),
                from,
                to,
            },
        )
        .await
    }

    async fn expect_transfer_error(state_manager: &mut StateManager, amount: &str, from: AccountRef, to: AccountRef, partial_error: &str) {
        let resp = transfer(state_manager, amount, from, to).await;
        let expected_error_raised = if let OperationResponse::Error(e) = resp {
            log::warn!("{}", e);
            e.contains(partial_error)
        } else {
            false
        };
        assert!(expected_error_raised);
    }

    #[tokio::test]
    async fn async_deposit() {
        let mut state_manager = get_fixture();
        let a12 = AccountRef {
            number: "a12".to_string(),
            new_version: Some(10),
        };

        let _ = send(
            &mut state_manager,
            AccountOperation::Deposit {
                amount: "10".to_string(),
                account: a12,
            },
        )
        .await;

        assert_eq!(state_manager.accounts.get(0).unwrap().balance.amount().to_string(), "101.00".to_string());
        assert_eq!(state_manager.accounts.get(0).unwrap().talos_state.version, 1);
        assert_eq!(state_manager.accounts.get(1).unwrap().balance.amount().to_string(), "112.00".to_string());
        assert_eq!(state_manager.accounts.get(1).unwrap().talos_state.version, 10);
        assert_eq!(state_manager.accounts.get(2).unwrap().balance.amount().to_string(), "103.00".to_string());
        assert_eq!(state_manager.accounts.get(2).unwrap().talos_state.version, 1);
    }

    #[tokio::test]
    async fn async_withdraw() {
        let mut state_manager = get_fixture();
        let a12 = AccountRef {
            number: "a12".to_string(),
            new_version: Some(10),
        };

        let _ = send(
            &mut state_manager,
            AccountOperation::Withdraw {
                amount: "10".to_string(),
                account: a12,
            },
        )
        .await;

        assert_eq!(state_manager.accounts.get(0).unwrap().balance.amount().to_string(), "101.00".to_string());
        assert_eq!(state_manager.accounts.get(0).unwrap().talos_state.version, 1);
        assert_eq!(state_manager.accounts.get(1).unwrap().balance.amount().to_string(), "92.00".to_string());
        assert_eq!(state_manager.accounts.get(1).unwrap().talos_state.version, 10);
        assert_eq!(state_manager.accounts.get(2).unwrap().balance.amount().to_string(), "103.00".to_string());
        assert_eq!(state_manager.accounts.get(2).unwrap().talos_state.version, 1);
    }

    #[tokio::test]
    async fn async_transfer() {
        let mut state_manager = get_fixture();
        let a12 = AccountRef {
            number: "a12".to_string(),
            new_version: Some(10),
        };
        let a13 = AccountRef {
            number: "a13".to_string(),
            new_version: Some(10),
        };

        let _ = transfer(&mut state_manager, "10", a12.clone(), a13.clone()).await;

        assert_eq!(state_manager.accounts.get(0).unwrap().balance.amount().to_string(), "101.00".to_string());
        assert_eq!(state_manager.accounts.get(0).unwrap().talos_state.version, 1);
        assert_eq!(state_manager.accounts.get(1).unwrap().balance.amount().to_string(), "92.00".to_string());
        assert_eq!(state_manager.accounts.get(1).unwrap().talos_state.version, 10);
        assert_eq!(state_manager.accounts.get(2).unwrap().balance.amount().to_string(), "113.00".to_string());
        assert_eq!(state_manager.accounts.get(2).unwrap().talos_state.version, 10);

        // Lets fail due to garbage money
        expect_transfer_error(&mut state_manager, "_", a12.clone(), a13.clone(), "Cannot create Money instance").await;

        // Lets fail due to bad target account
        expect_transfer_error(&mut state_manager, "10", a12.clone(), get_bad_account(), "No such account").await;

        // Lets fail due to bad source account
        expect_transfer_error(&mut state_manager, "10", get_bad_account(), a13.clone(), "No such account").await;
    }

    #[tokio::test]
    async fn async_query_all() {
        let mut state_manager = get_fixture();
        let resp = send(&mut state_manager, AccountOperation::QueryAll).await;
        let expected_resp = if let OperationResponse::QueryResult(data) = resp {
            assert!(data.is_some());
            let list = data.unwrap();
            assert_eq!(list.get(0).unwrap().number, "a11".to_string());
            assert_eq!(list.get(1).unwrap().number, "a12".to_string());
            assert_eq!(list.get(2).unwrap().number, "a13".to_string());
            true
        } else {
            false
        };

        assert!(expected_resp);
    }

    #[tokio::test]
    async fn async_query_one() {
        let mut state_manager = get_fixture();
        let a12 = AccountRef {
            number: "a12".to_string(),
            new_version: None,
        };
        let resp = send(&mut state_manager, AccountOperation::QueryAccount { account: a12 }).await;
        let expected_resp = if let OperationResponse::QueryResult(data) = resp {
            assert!(data.is_some());
            let list = data.unwrap();
            assert_eq!(list.get(0).unwrap().number, "a12".to_string());
            assert_eq!(1, list.len());
            true
        } else {
            false
        };
        assert!(expected_resp);

        let resp = send(&mut state_manager, AccountOperation::QueryAccount { account: get_bad_account() }).await;
        let expected_resp = if let OperationResponse::QueryResult(data) = resp {
            data.is_none()
        } else {
            false
        };

        assert!(expected_resp);
    }

    #[tokio::test]
    async fn async_query_snapshot() {
        let mut state_manager = get_fixture();
        let resp = send(&mut state_manager, AccountOperation::QuerySnapshot).await;
        let expected_resp = if let OperationResponse::Snapshot(data) = resp {
            assert_eq!(data.version, 1);
            true
        } else {
            false
        };

        assert!(expected_resp);
    }

    #[tokio::test]
    async fn async_update_snapshot() {
        let mut state_manager = get_fixture();
        let _ = send(&mut state_manager, AccountOperation::UpdateSnapshot(11)).await;
        assert_eq!(state_manager.snapshot.version, 11);
    }
}
// $coverage:ignore-end
