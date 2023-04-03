use std::ops::Mul;

use tokio::sync::mpsc::Receiver;

use crate::model::bank_account::{as_money, BankAccount};
use crate::state::model::{AccountOperation, AccountRef, Envelope, OperationResponse};

pub struct StateManager {
    pub accounts: Vec<BankAccount>,
}

impl StateManager {
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
            Self::deposit_to_account(amount, account)
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

    fn deposit_to_account(amount: String, account: &mut BankAccount) -> OperationResponse {
        match as_money(amount, account.balance.currency()) {
            Ok(amount) => {
                let a = amount.clone();
                log::debug!("Deposit {:>8}{} >>> {}", a.amount(), a.currency().iso_alpha_code, account.number);
                account.increment(amount);
                OperationResponse::Success
            }
            Err(e) => OperationResponse::Error(e),
        }
    }

    fn withdraw_from_account(amount: String, account: &mut BankAccount) -> OperationResponse {
        match as_money(amount, account.balance.currency()) {
            Ok(amount) => {
                let a = amount.clone();
                log::debug!("Withdraw {:>7}{} <<< {}", a.amount(), a.currency().iso_alpha_code, account.number);
                account.increment(amount.mul(-1));
                OperationResponse::Success
            }
            Err(e) => OperationResponse::Error(e),
        }
    }
}
