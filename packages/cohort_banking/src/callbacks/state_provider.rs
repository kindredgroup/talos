use std::sync::Arc;

use async_trait::async_trait;
use cohort_sdk::model::callbacks::{CapturedItemState, CapturedState, ItemStateProvider};
use rust_decimal::Decimal;
use tokio_postgres::Row;

use crate::{
    model::{bank_account::BankAccount, requests::TransferRequest},
    state::postgres::database::{Database, DatabaseError},
};

pub struct StateProviderImpl {
    pub request: TransferRequest,
    pub database: Arc<Database>,
    pub single_query_strategy: bool,
}

impl StateProviderImpl {
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

    async fn get_state_using_two_queries(&self) -> Result<CapturedState, String> {
        let list = self
            .database
            .query_many(
                r#"SELECT ba.* FROM bank_accounts ba WHERE ba."number" = $1 OR ba."number" = $2"#,
                &[&self.request.from, &self.request.to],
                Self::account_from_row,
            )
            .await
            .map_err(|e| e.to_string())?;

        if list.len() != 2 {
            return Err(format!("Unable to load state of accounts: '{}' and '{}'", self.request.from, self.request.to));
        }

        let snapshot_version = self
            .database
            .query_one(
                r#"SELECT cs."version" AS snapshot_version FROM cohort_snapshot cs WHERE cs.id = $1"#,
                &[&"SINGLETON"],
                |row| {
                    let snapshot_version = row
                        .try_get::<&str, i64>("snapshot_version")
                        .map_err(|e| DatabaseError::deserialise_payload(e.to_string(), "Cannot read snapshot_version".into()))?;
                    Ok(snapshot_version as u64)
                },
            )
            .await
            .map_err(|e| e.to_string())?;

        Ok(CapturedState {
            snapshot_version,
            items: list
                .iter()
                .map(|account| CapturedItemState {
                    id: account.number.clone(),
                    version: account.version,
                })
                .collect(),
            abort_reason: None,
        })
    }

    async fn get_state_using_one_query(&self) -> Result<CapturedState, String> {
        let list = self
            .database
            .query_many(
                // Note:
                // We intentioanly left 'cohort_snapshot' table not joined to 'bank_accounts', in that case
                // database will multiply its content with 'bank_accounts' and automatically join rows from both tables.
                // Given that 'cohort_snapshot' will ever have one single row, we are good here.
                r#"
                SELECT
                    ba.*, cs."version" AS snapshot_version
                FROM
                    bank_accounts ba, cohort_snapshot cs
                WHERE
                    ba."number" = $1 OR ba."number" = $2"#,
                &[&self.request.from, &self.request.to],
                // convert RAW output into tuple (bank account, snap ver)
                |row| {
                    let account = Self::account_from_row(row)?;
                    let snapshot_version = row
                        .try_get::<&str, i64>("snapshot_version")
                        .map_err(|e| DatabaseError::deserialise_payload(e.to_string(), "Cannot read snapshot_version".into()))?;
                    Ok((account, snapshot_version as u64))
                },
            )
            .await
            .map_err(|e| e.to_string())?;

        if list.len() != 2 {
            return Err(format!("Unable to load state of accounts: '{}' and '{}'", self.request.from, self.request.to));
        }

        Ok(CapturedState {
            snapshot_version: list[0].1,
            items: list
                .iter()
                .map(|tuple| CapturedItemState {
                    id: tuple.0.number.clone(),
                    version: tuple.0.version,
                })
                .collect(),
            abort_reason: None,
        })
    }
}

#[async_trait]
impl ItemStateProvider for StateProviderImpl {
    async fn get_state(&self) -> Result<CapturedState, String> {
        if self.single_query_strategy {
            self.get_state_using_one_query().await
        } else {
            self.get_state_using_two_queries().await
        }
    }
}
