use std::sync::Arc;

use rusty_money::iso::Currency;
use rusty_money::Money;

use crate::actions::account_update::AccountUpdate;
use crate::actions::transfer::Transfer;
use crate::model::bank_account::BankAccount;
use crate::model::requests::AccountUpdateRequest;
use crate::state::postgres::data_store::DataStore;
use crate::state::postgres::database::{Action, Database};

pub struct BankApi {}

impl BankApi {
    pub async fn get_accounts(db: Arc<Database>) -> Result<Vec<BankAccount>, String> {
        let list = db.query("SELECT data FROM bank_accounts", DataStore::account_from_row).await;
        Ok(list)
    }

    pub async fn get_balance(db: Arc<Database>, account: String) -> Result<Money<'static, Currency>, String> {
        let rslt = db
            .query_opt("SELECT data FROM bank_accounts WHERE number = $1", &[&account], DataStore::account_from_row)
            .await;

        match rslt {
            Some(account) => Ok(account.balance),
            None => Err(format!("There is no bank account with number: {}", account)),
        }
    }

    pub async fn deposit(db: Arc<Database>, data: AccountUpdateRequest, new_version: u64) -> Result<u64, String> {
        AccountUpdate::deposit(data, new_version).execute(&db.pool.get().await.unwrap()).await
    }

    pub async fn withdraw(db: Arc<Database>, data: AccountUpdateRequest, new_version: u64) -> Result<u64, String> {
        Self::deposit(db, data, new_version).await
    }

    pub async fn transfer(db: Arc<Database>, from: String, to: String, amount: String, new_version: u64) -> Result<u64, String> {
        let affected_rows = Self::transfer_one(db, Transfer::new(from.clone(), to.clone(), amount.clone(), new_version)).await?;
        if affected_rows == 0 || affected_rows == 2 {
            // If 0 then someone else has installed this already, still a valid state
            Ok(affected_rows)
        } else {
            Err(format!(
                "Unable to transfer ${} from '{}' to '{}'. Error: affected rows({}) != 2",
                amount, from, to, affected_rows,
            ))
        }
    }

    pub async fn transfer_one(db: Arc<Database>, action: Transfer) -> Result<u64, String> {
        action.execute(&db.pool.get().await.unwrap()).await
    }
}
