use std::collections::HashMap;
use std::sync::Arc;

use rusty_money::iso::Currency;
use rusty_money::Money;

use crate::actions::account_update::AccountUpdate;
use crate::actions::action::Action;
use crate::actions::transfer::Transfer;

use crate::model::bank_account::BankAccount;
use crate::model::requests::{AccountUpdateRequest, TransferRequest};

use crate::state::postgres::data_access::PostgresApi;
use crate::state::postgres::data_store::DataStore;
use crate::state::postgres::database::Database;

use crate::state::data_access_api::{ManualTx, TxApi};

pub struct BankApi {}

impl BankApi {
    pub async fn get_accounts(db: Arc<Database>) -> Result<Vec<BankAccount>, String> {
        let list = db.query("SELECT data FROM bank_accounts", DataStore::account_from_row).await;
        Ok(list)
    }

    pub async fn get_accounts_as_map(db: Arc<Database>) -> Result<HashMap<String, BankAccount>, String> {
        let list = db.query("SELECT data FROM bank_accounts", DataStore::account_from_row).await;
        let mut map = HashMap::<String, BankAccount>::new();
        for account in list.iter() {
            map.insert(account.number.clone(), account.clone());
        }
        Ok(map)
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
        let mut manual_tx_api = PostgresApi { client: db.get().await };
        let tx = manual_tx_api.transaction().await;
        let action = AccountUpdate::deposit(data, new_version);
        action.execute(&tx).await.as_ref()?;
        let result = action.update_version(&tx).await;
        if result.is_ok() {
            let rslt_commit = tx.commit().await;
            if let Err(e) = rslt_commit {
                return Err(format!("Unable to commit: {}. The original transaction updated: {} rows", e, result.unwrap()));
            }
        }

        result
    }

    pub async fn withdraw(db: Arc<Database>, data: AccountUpdateRequest, new_version: u64) -> Result<u64, String> {
        let mut manual_tx_api = PostgresApi { client: db.get().await };
        let tx = manual_tx_api.transaction().await;
        let action = AccountUpdate::withdraw(data, new_version);
        action.execute(&tx).await.as_ref()?;
        let result = action.update_version(&tx).await;
        if result.is_ok() {
            let rslt_commit = tx.commit().await;
            if let Err(e) = rslt_commit {
                return Err(format!("Unable to commit: {}. The original transaction updated: {} rows", e, result.unwrap()));
            }
        }
        result
    }

    pub async fn transfer(db: Arc<Database>, data: TransferRequest, new_version: u64) -> Result<u64, String> {
        let affected_rows = Self::transfer_one(db, Transfer::new(data.clone(), new_version)).await?;
        if affected_rows == 0 || affected_rows == 2 {
            // If 0 then someone else has installed this already, still a valid state
            Ok(affected_rows)
        } else {
            Err(format!(
                "Unable to transfer ${} from '{}' to '{}'. Error: affected rows({}) != 2",
                data.from, data.to, data.amount, affected_rows,
            ))
        }
    }

    pub async fn transfer_one(db: Arc<Database>, action: Transfer) -> Result<u64, String> {
        let mut manual_tx_api = PostgresApi { client: db.get().await };
        let tx = manual_tx_api.transaction().await;
        action.execute(&tx).await.as_ref()?;
        let result = action.update_version(&tx).await;
        if result.is_ok() {
            let rslt_commit = tx.commit().await;
            if let Err(e) = rslt_commit {
                return Err(format!("Unable to commit: {}. The original transaction updated: {} rows", e, result.unwrap()));
            }
        }
        result
    }
}
