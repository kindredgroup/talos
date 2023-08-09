use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use deadpool_postgres::Transaction;
use talos_cohort_replicator::{ReplicatorInstallStatus, ReplicatorInstaller, StatemapItem};
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

/// This function returns a closure to be used to check when there is an error,
/// whether retry is allowed, along with the number of retry attempts
fn retry_allowed_on_error(max_retry: u32) -> impl FnMut(&str) -> Option<u32> {
    let mut remaining_retries = max_retry;
    move |err: &str| {
        if err.contains("could not serialize access due to concurrent update") {
            remaining_retries -= 1;
            Some(remaining_retries)
        } else {
            None
        }
    }
}
pub struct BankStatemapInstaller {
    pub database: Arc<Database>,
    pub max_retry: u32,
    pub retry_wait_ms: u64,
}

impl BankStatemapInstaller {
    async fn update_bank_transfer_request(tx: &Transaction<'_>, statemap: &[StatemapItem], _snapshot_version: u64) -> Result<u64, String> {
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
    /// Certain errors like `could not serialize access due to concurrent update` in postgres are retryable.
    /// - Updating `bank_account` workflow.
    ///     - On successful completion, we proceed to update the `snapshot_version` in `cohort_snapshot`.
    ///     - When there is a retryable error, we wait for `retry_wait_ms` milliseconds and go to the start of the loop to retry. `(Txn aborts and retries)`
    ///     - If all the retries are exhausted, we return `ReplicatorInstallStatus::Gaveup`. `(Txn aborts and returns)`
    ///     - For non-retryable error, we return the `Error`.
    /// - Updating `cohort_snapshot` workflow.
    ///     - On successful completion, we commit the transaction and return `ReplicatorInstallStatus::Installed`. `(Txn Commits and returns)`
    ///     - When there is a retryable error, we wait for `retry_wait_ms` milliseconds and go to the start of the loop to retry. `(Txn aborts and retries)`
    ///     - If all the retries are exhausted, we return `ReplicatorInstallStatus::InstalledWithoutSnapshotUpdate`, denoting a updates for bank_transfer went through but snapshot wasn't updated. `(Txn Commits and returns)`
    ///     - For non-retryable error, we return the `Error`.

    async fn install(&self, statemap: Vec<StatemapItem>, snapshot_version: u64) -> Result<ReplicatorInstallStatus, String> {
        let mut bank_transfer_error_retry_check = retry_allowed_on_error(self.max_retry);
        let mut snapshot_retry_check = retry_allowed_on_error(self.max_retry);

        let mut cnn = self.database.get().await.map_err(|e| e.to_string())?;
        loop {
            let tx = cnn.transaction().await.map_err(|e| e.to_string())?;
            if !statemap.is_empty() {
                let updated_rows_res = BankStatemapInstaller::update_bank_transfer_request(&tx, &statemap, snapshot_version).await;

                match updated_rows_res {
                    Ok(updated_rows) => {
                        if updated_rows > 0 {
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
                        match bank_transfer_error_retry_check(&bank_transfer_db_error) {
                            Some(remaining_count) => {
                                if remaining_count > 0 {
                                    tokio::time::sleep(Duration::from_millis(self.retry_wait_ms)).await;
                                    continue;
                                }

                                return Ok(ReplicatorInstallStatus::Gaveup(remaining_count));
                            }
                            None => {
                                return Err(bank_transfer_db_error);
                            }
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

                    return Ok(ReplicatorInstallStatus::Installed);
                }
                Err(error) => match snapshot_retry_check(&error) {
                    Some(remaining_count) => {
                        if remaining_count > 0 {
                            tokio::time::sleep(Duration::from_millis(self.retry_wait_ms)).await;
                            continue;
                        }

                        return Ok(ReplicatorInstallStatus::InstalledWithoutSnapshotUpdate);
                    }
                    None => {
                        return Err(error);
                    }
                },
            };
        }
    }
}
