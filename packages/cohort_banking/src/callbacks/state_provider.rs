use std::sync::Arc;

use async_trait::async_trait;
use cohort_sdk::model::callbacks::{CapturedItemState, CapturedState, ItemStateProvider};

use crate::{
    bank_api::BankApi,
    model::requests::TransferRequest,
    state::postgres::database::{Database, DatabaseError},
};

pub struct StateProviderImpl {
    pub request: TransferRequest,
    pub database: Arc<Database>,
}

#[async_trait]
impl ItemStateProvider for StateProviderImpl {
    async fn get_state(&self) -> Result<CapturedState, String> {
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
                    let account = BankApi::account_from_row(row)?;
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
        })
    }
}
