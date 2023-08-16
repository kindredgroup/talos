use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use deadpool_postgres::Transaction;
use talos_cohort_replicator::{ReplicatorInstaller, StatemapItem};
use tokio_postgres::types::ToSql;

use crate::{model::requests::TransferRequest, state::postgres::database::Database};

const BANK_ACCOUNTS_UPDATE_QUERY: &str = r#"
UPDATE bank_accounts ba SET
"amount" =
(CASE
    WHEN ba."number" = ($1)::TEXT THEN ba."amount" + ($3)::DECIMAL
    WHEN ba."number" = ($2)::TEXT THEN ba."amount" - ($3)::DECIMAL
    END),
    "version" = ($4)::BIGINT
    WHERE ba."number" IN (($1)::TEXT, ($2)::TEXT)
    AND ba."version" < ($4)::BIGINT
    "#;

const SNAPSHOT_UPDATE_QUERY: &str = r#"UPDATE cohort_snapshot SET "version" = ($1)::BIGINT WHERE id = $2 AND "version" < ($1)::BIGINT"#;

fn is_retriable_error(error: &str) -> bool {
    // TODO: improve retriable error detection when error moves from String to enum or struct.
    error.contains("could not serialize access due to concurrent update")
}

pub struct BankStatemapInstaller {
    pub database: Arc<Database>,
    pub max_retry: u32,
    pub retry_wait_ms: u64,
}

impl BankStatemapInstaller {
    async fn install_bank_transfer_statemap(tx: &Transaction<'_>, statemap: &[StatemapItem], _snapshot_version: u64) -> Result<u64, String> {
        let sti = statemap[0].clone();

        let request: TransferRequest = serde_json::from_value(sti.payload.clone()).map_err(|e| e.to_string())?;
        let params: &[&(dyn ToSql + Sync)] = &[&request.from, &request.to, &request.amount, &(sti.version as i64)];

        let updated_rows = tx.execute(BANK_ACCOUNTS_UPDATE_QUERY, params).await.map_err(|e| e.to_string())?;
        Ok(updated_rows)
    }

    async fn update_snapshot(tx: &Transaction<'_>, snapshot_version: u64) -> Result<u64, String> {
        let params: &[&(dyn ToSql + Sync)] = &[&(snapshot_version as i64), &"SINGLETON"];

        tx.execute(SNAPSHOT_UPDATE_QUERY, params).await.map_err(|e| e.to_string())
    }
}

#[async_trait]
impl ReplicatorInstaller for BankStatemapInstaller {
    /// Install statemaps and version for respective rows in the `bank_accounts` table and update the `snapshot_version` in `cohort_snapshot` table.
    ///
    /// Certain errors like `could not serialize access due to concurrent update` in postgres are retriable.
    /// - Updating `bank_account` workflow.
    ///     - On successful completion, we proceed to update the `snapshot_version` in `cohort_snapshot`.
    ///     - When there is a retryable error, go to the start of the loop to retry. `(Txn aborts and retries)`
    ///     - For non-retryable error, we return the `Error`.
    /// - Updating `cohort_snapshot` workflow.
    ///     - On successful completion, we commit the transaction and return. `(Txn Commits and returns)`
    ///     - When there is a retryable error, go to the start of the loop to retry. `(Txn aborts and retries)`
    ///     - For non-retryable error, we return the `Error`.
    async fn install(&self, statemap: Vec<StatemapItem>, snapshot_version: u64) -> Result<(), String> {
        let mut cnn = self.database.get().await.map_err(|e| e.to_string())?;
        loop {
            let tx = cnn.transaction().await.map_err(|e| e.to_string())?;
            if !statemap.is_empty() {
                let updated_rows_res = BankStatemapInstaller::install_bank_transfer_statemap(&tx, &statemap, snapshot_version).await;

                match updated_rows_res {
                    Ok(updated_rows) => {
                        if updated_rows == 0 {
                            log::debug!(
                                "No rows were updated when installing: {:?}. Snapshot will be set to: {}",
                                statemap,
                                snapshot_version
                            );
                        }

                        log::info!(
                            "{} rows were updated when installing: {:?}. Snapshot will be set to: {}",
                            updated_rows,
                            statemap,
                            snapshot_version
                        );
                    }
                    Err(bank_transfer_db_error) => {
                        //  Check if retry is allowed on the error.
                        if is_retriable_error(&bank_transfer_db_error) {
                            tokio::time::sleep(Duration::from_millis(self.retry_wait_ms)).await;
                            continue;
                        } else {
                            return Err(bank_transfer_db_error);
                        }
                    }
                }
            }

            let result = BankStatemapInstaller::update_snapshot(&tx, snapshot_version).await;

            match result {
                Ok(updated_rows) => {
                    if updated_rows == 0 {
                        log::debug!(
                            "No rows were updated when updating snapshot. Snapshot is already set to {} or higher",
                            snapshot_version
                        );
                    }

                    log::info!("{} rows were updated when updating snapshot to {}", updated_rows, snapshot_version);

                    tx.commit()
                        .await
                        .map_err(|tx_error| format!("Commit error for statemap. Error: {}", tx_error))?;

                    return Ok(());
                }
                Err(error) =>
                //  Check if retry is allowed on the error.
                {
                    if is_retriable_error(&error) {
                        tokio::time::sleep(Duration::from_millis(self.retry_wait_ms)).await;
                        continue;
                    } else {
                        return Err(error);
                    }
                }
            };
        }
    }
}
