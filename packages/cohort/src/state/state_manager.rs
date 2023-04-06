use std::ops::Mul;

use tokio::sync::mpsc::Receiver;

use crate::model::bank_account::{as_money, BankAccount};
use crate::state::model::{AccountOperation, AccountRef, Envelope, OperationResponse};

pub struct StateManager {
    pub accounts: Vec<BankAccount>,
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
            AccountOperation::QueryAll => OperationResponse::QueryResult(Some(self.accounts.clone())),

            AccountOperation::QueryAccount { account: account_ref } => {
                let account = self.accounts.iter().find(|a| a.number == account_ref.number);
                if account.is_none() {
                    OperationResponse::QueryResult(None)
                } else {
                    OperationResponse::QueryResult(Some(vec![account.unwrap().clone()]))
                }
            }

            AccountOperation::Deposit { amount, account: account_ref } => Self::deposit(amount, account_ref, &mut self.accounts),

            AccountOperation::Withdraw { amount, account: account_ref } => Self::withdraw(amount, account_ref, &mut self.accounts),

            AccountOperation::Transfer { amount, from, to } => {
                if let OperationResponse::Error(e) = Self::deposit(amount.clone(), to.clone(), &mut self.accounts) {
                    OperationResponse::Error(format!("Unable to deposit {} to {}. Error: {}", amount, to, e))
                } else if let OperationResponse::Error(e) = Self::withdraw(amount.clone(), from.clone(), &mut self.accounts) {
                    OperationResponse::Error(format!("Unable to withdraw {} from {}. Error: {}", amount, from, e))
                } else {
                    OperationResponse::Success
                }
            }
        };

        request.tx_reply.send(answer).unwrap();
    }

    fn find_account(account_ref: AccountRef, all_accounts: &mut [BankAccount]) -> Result<&mut BankAccount, String> {
        for account in &mut all_accounts.iter_mut() {
            if account.number == account_ref.number {
                return Ok(account);
            }
        }

        Err(format!("No such account {}", account_ref))
    }

    fn deposit(amount: String, account_ref: AccountRef, all_accounts: &mut [BankAccount]) -> OperationResponse {
        if let Ok(account) = Self::find_account(account_ref.clone(), all_accounts) {
            Self::deposit_to_account(amount, account, account_ref.new_version)
        } else {
            OperationResponse::Error(format!("No such account {}", account_ref))
        }
    }

    fn withdraw(amount: String, account_ref: AccountRef, all_accounts: &mut [BankAccount]) -> OperationResponse {
        if let Ok(account) = Self::find_account(account_ref.clone(), all_accounts) {
            Self::withdraw_from_account(amount, account)
        } else {
            OperationResponse::Error(format!("No such account {}", account_ref))
        }
    }

    fn deposit_to_account(amount: String, account: &mut BankAccount, new_version: Option<u64>) -> OperationResponse {
        match as_money(amount, account.balance.currency()) {
            Ok(amount) => {
                account.increment(amount);
                if let Some(version) = new_version {
                    account.talos_state.version = version;
                }
                OperationResponse::Success
            }
            Err(e) => OperationResponse::Error(e),
        }
    }

    fn withdraw_from_account(amount: String, account: &mut BankAccount) -> OperationResponse {
        match as_money(amount, account.balance.currency()) {
            Ok(amount) => {
                account.increment(amount.mul(-1));
                OperationResponse::Success
            }
            Err(e) => OperationResponse::Error(e),
        }
    }
}

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::talos_state::TalosState;

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
        StateManager { accounts: get_test_accounts() }
    }

    async fn send(state_manager: &mut StateManager, req: AccountOperation) -> OperationResponse {
        let (tx, rx) = tokio::sync::oneshot::channel::<OperationResponse>();
        let req = Envelope { data: req, tx_reply: tx };
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

    #[test]
    fn find_account() {
        let mut list = get_test_accounts();
        let result = StateManager::find_account(
            AccountRef {
                number: "a12".to_string(),
                new_version: None,
            },
            &mut list,
        );

        assert!(result.is_ok());
        let result = StateManager::find_account(get_bad_account(), &mut list);
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e, "No such account AccountRef: [number: _, new_version: None]");
        }
    }

    #[test]
    fn deposit() {
        let mut list = get_test_accounts();
        let a12 = AccountRef {
            number: "a12".to_string(),
            new_version: None,
        };
        let result = StateManager::deposit("10".to_string(), a12.clone(), &mut list);

        assert_eq!(result, OperationResponse::Success);
        assert_eq!(list.get(0).unwrap().balance.amount().to_string(), "101.00".to_string());
        assert_eq!(list.get(1).unwrap().balance.amount().to_string(), "112.00".to_string());
        assert_eq!(list.get(2).unwrap().balance.amount().to_string(), "103.00".to_string());

        let result = StateManager::deposit("10".to_string(), get_bad_account(), &mut list);
        assert_eq!(
            result,
            OperationResponse::Error("No such account AccountRef: [number: _, new_version: None]".to_string())
        );

        let result = StateManager::deposit("_".to_string(), a12, &mut list);
        let expected_error_raised = if let OperationResponse::Error(e) = result {
            e.starts_with("Cannot create Money instance")
        } else {
            false
        };

        assert!(expected_error_raised);
    }

    #[test]
    fn withdraw() {
        let mut list = get_test_accounts();
        let a12 = AccountRef {
            number: "a12".to_string(),
            new_version: None,
        };

        let result = StateManager::withdraw("10".to_string(), a12.clone(), &mut list);

        assert_eq!(result, OperationResponse::Success);
        assert_eq!(list.get(0).unwrap().balance.amount().to_string(), "101.00".to_string());
        assert_eq!(list.get(1).unwrap().balance.amount().to_string(), "92.00".to_string());
        assert_eq!(list.get(2).unwrap().balance.amount().to_string(), "103.00".to_string());

        let result = StateManager::withdraw("10".to_string(), get_bad_account(), &mut list);
        assert_eq!(
            result,
            OperationResponse::Error("No such account AccountRef: [number: _, new_version: None]".to_string())
        );

        let result = StateManager::withdraw("_".to_string(), a12, &mut list);
        let expected_error_raised = if let OperationResponse::Error(e) = result {
            e.starts_with("Cannot create Money instance")
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
            new_version: None,
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
        assert_eq!(state_manager.accounts.get(1).unwrap().balance.amount().to_string(), "112.00".to_string());
        assert_eq!(state_manager.accounts.get(2).unwrap().balance.amount().to_string(), "103.00".to_string());
    }

    #[tokio::test]
    async fn async_withdraw() {
        let mut state_manager = get_fixture();
        let a12 = AccountRef {
            number: "a12".to_string(),
            new_version: None,
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
        assert_eq!(state_manager.accounts.get(1).unwrap().balance.amount().to_string(), "92.00".to_string());
        assert_eq!(state_manager.accounts.get(2).unwrap().balance.amount().to_string(), "103.00".to_string());
    }

    #[tokio::test]
    async fn async_transfer() {
        let mut state_manager = get_fixture();
        let a12 = AccountRef {
            number: "a12".to_string(),
            new_version: None,
        };
        let a13 = AccountRef {
            number: "a13".to_string(),
            new_version: None,
        };

        let _ = transfer(&mut state_manager, "10", a12.clone(), a13.clone()).await;

        assert_eq!(state_manager.accounts.get(0).unwrap().balance.amount().to_string(), "101.00".to_string());
        assert_eq!(state_manager.accounts.get(1).unwrap().balance.amount().to_string(), "92.00".to_string());
        assert_eq!(state_manager.accounts.get(2).unwrap().balance.amount().to_string(), "113.00".to_string());

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
}
// $coverage:ignore-end
