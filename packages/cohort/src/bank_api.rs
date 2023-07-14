use std::collections::HashMap;
use std::sync::Arc;

use rust_decimal::Decimal;
use tokio_postgres::Row;

use crate::actions::action::Action;
use crate::actions::transfer::Transfer;

use crate::model::bank_account::BankAccount;
use crate::model::requests::TransferRequest;

use crate::state::postgres::data_access::PostgresApi;
use crate::state::postgres::database::{Database, DatabaseError};

use crate::state::data_access_api::{ManualTx, TxApi};

pub struct BankApi {}

impl BankApi {
    pub fn account_from_row(row: &Row) -> Result<BankAccount, DatabaseError> {
        Ok(BankAccount {
            name: row
                .try_get::<&str, String>("name")
                .map_err(|e| DatabaseError::deserialise_payload(e.to_string(), "Cannot read account name".into()))?,
            number: row
                .try_get::<&str, String>("number")
                .map_err(|e| DatabaseError::deserialise_payload(e.to_string(), "Cannot read account number".into()))?,
            version: row
                .try_get::<&str, i64>("version")
                .map_err(|e| DatabaseError::deserialise_payload(e.to_string(), "Cannot read account version".into()))? as u64,
            balance: row
                .try_get::<&str, Decimal>("amount")
                .map_err(|e| DatabaseError::deserialise_payload(e.to_string(), "Cannot read account amount".into()))?,
        })
    }

    pub async fn get_accounts(db: Arc<Database>) -> Result<Vec<BankAccount>, DatabaseError> {
        db.query("SELECT * FROM bank_accounts", Self::account_from_row).await
    }

    pub async fn get_accounts_as_map(db: Arc<Database>, number_from: String, number_to: String) -> Result<HashMap<String, BankAccount>, DatabaseError> {
        let mut map = HashMap::<String, BankAccount>::new();

        let from = db
            .query_one("SELECT * FROM bank_accounts WHERE number = $1", &[&number_from], BankApi::account_from_row)
            .await?;
        let to = db
            .query_one("SELECT * FROM bank_accounts WHERE number = $1", &[&number_to], BankApi::account_from_row)
            .await?;
        map.insert(number_from, from);
        map.insert(number_to, to);

        Ok(map)
    }

    pub async fn transfer(db: Arc<Database>, data: TransferRequest, new_version: u64) -> Result<u64, String> {
        let affected_rows = Self::transfer_one(db, Transfer::new(data.clone(), new_version)).await?;
        if affected_rows == 0 || affected_rows == 2 {
            // If 0 then someone else has installed this already, still a valid state
            Ok(affected_rows)
        } else {
            Err(format!(
                "Unable to transfer ${} from '{}' to '{}'. Error: affected rows({}) != 2",
                data.amount, data.from, data.to, affected_rows,
            ))
        }
    }

    pub async fn transfer_one(db: Arc<Database>, action: Transfer) -> Result<u64, String> {
        let mut manual_tx_api = PostgresApi {
            client: db.get().await.map_err(|e| e.to_string())?,
        };
        let tx = manual_tx_api.transaction().await;
        action.execute(&tx).await.as_ref()?;
        let result = action.update_version(&tx).await;
        if result.is_ok() {
            let rslt_commit = tx.commit().await;
            if let Err(e) = rslt_commit {
                return Err(format!("Unable to commit: {}. The original transaction updated: {} rows", e, result.unwrap()));
            }
        } else {
            let _ = tx.rollback().await;
        }
        result
    }
}
