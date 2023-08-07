use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use talos_cohort_replicator::{ReplicatorInstallStatus, ReplicatorInstaller, StatemapItem};
use tokio_postgres::types::ToSql;

use crate::{model::requests::TransferRequest, state::postgres::database::Database};

pub struct StatemapInstallerImpl {
    pub database: Arc<Database>,
    pub max_retry: u32,
    pub retry_wait_ms: u64,
}

#[async_trait]
impl ReplicatorInstaller for StatemapInstallerImpl {
    async fn install(&self, statemap: Vec<StatemapItem>, snapshot_version: u64) -> Result<ReplicatorInstallStatus, String> {
        let mut current_count = 0;

        loop {
            // from      = 1
            // to        = 2
            // amount    = 3
            // new_ver   = 4

            let mut cnn = self.database.get().await.map_err(|e| e.to_string())?;
            let tx = cnn.transaction().await.map_err(|e| e.to_string())?;

            if !statemap.is_empty() {
                let sti = statemap[0].clone();

                let request: TransferRequest = serde_json::from_value(sti.payload.clone()).map_err(|e| e.to_string())?;

                let sql = r#"
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

                let params: &[&(dyn ToSql + Sync)] = &[&request.from, &request.to, &request.amount, &(sti.version as i64)];

                let updated_rows = tx.execute(sql, params).await.map_err(|e| e.to_string())?;

                if updated_rows > 0 {
                    log::debug!("No rows were updated when installing: {:?}. Snapshot will be set to: {}", sti, snapshot_version);
                }

                log::info!(
                    "{} rows were updated when installing: {:?}. Snapshot will be set to: {}",
                    updated_rows,
                    sti,
                    snapshot_version
                );
            }

            let params: &[&(dyn ToSql + Sync)] = &[&(snapshot_version as i64), &"SINGLETON"];

            let sql = r#"UPDATE cohort_snapshot SET "version" = ($1)::BIGINT WHERE id = $2 AND "version" < ($1)::BIGINT"#;
            let result = tx.execute(sql, params).await;

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

                    return Ok(ReplicatorInstallStatus::Success);
                }
                Err(error) => {
                    if error.to_string().contains("could not serialize access due to concurrent update") {
                        current_count += 1;

                        if current_count >= self.max_retry {
                            return Ok(ReplicatorInstallStatus::Gaveup(current_count));
                        }

                        // If serialize error and haven't reached the retry limit, sleep for 2 ms.
                        tokio::time::sleep(Duration::from_millis(self.retry_wait_ms)).await;
                    } else {
                        return Err(error.to_string());
                    }
                }
            }
        }
    }
}