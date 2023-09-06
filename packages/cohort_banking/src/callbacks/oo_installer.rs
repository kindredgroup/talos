use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use cohort_sdk::model::callback::{OutOfOrderInstallOutcome, OutOfOrderInstallRequest, OutOfOrderInstaller};
use opentelemetry_api::metrics::Counter;
use tokio_postgres::types::ToSql;

use crate::{
    model::requests::TransferRequest,
    state::postgres::database::{Database, DatabaseError},
};

pub struct OutOfOrderInstallerImpl {
    pub database: Arc<Database>,
    pub detailed_logging: bool,
    pub counter_oo_no_data_found: Arc<Counter<u64>>,
    pub single_query_strategy: bool,
}

pub static SNAPSHOT_SINGLETON_ROW_ID: &str = "SINGLETON";

impl OutOfOrderInstallerImpl {
    async fn install_using_single_query(
        &self,
        xid: &str,
        safepoint: u64,
        new_version: u64,
        request: &TransferRequest,
    ) -> Result<OutOfOrderInstallOutcome, String> {
        // Params order:
        //  1 - from, 2 - to, 3 - amount
        //  4 - new_ver, 5 - safepoint
        let sql = r#"
        WITH bank_accounts_temp AS (
            UPDATE bank_accounts ba SET
                "amount" =
                    (CASE
                        WHEN ba."number" = ($1)::TEXT THEN ba."amount" + ($3)::DECIMAL
                        WHEN ba."number" = ($2)::TEXT THEN ba."amount" - ($3)::DECIMAL
                    END),
                "version" = ($4)::BIGINT
            WHERE ba."number" IN (($1)::TEXT, ($2)::TEXT)
                AND EXISTS (SELECT 1 FROM cohort_snapshot cs WHERE cs."version" >= ($5)::BIGINT)
                AND ba."version" < ($4)::BIGINT
            RETURNING
                ba."number", ba."version" as "new_version", (null)::BIGINT as "version", (SELECT cs."version" FROM cohort_snapshot cs) as "snapshot"
        )
        SELECT * FROM bank_accounts_temp
        UNION
        SELECT
            ba."number", (null)::BIGINT as "new_version", ba."version" as "version", cs."version" as "snapshot"
        FROM
            bank_accounts ba, cohort_snapshot cs
        WHERE ba."number" IN (($1)::TEXT, ($2)::TEXT)
            "#;

        let params: &[&(dyn ToSql + Sync)] = &[&request.from, &request.to, &request.amount, &(new_version as i64), &(safepoint as i64)];

        let result = self
            .database
            .query_many(sql, params, |row| {
                let nr = row
                    .try_get::<&str, String>("number")
                    .map_err(|e| DatabaseError::deserialise_payload(e.to_string(), "Cannot read account name".into()))?;
                let new_ver = row
                    .try_get::<&str, Option<i64>>("new_version")
                    .map_err(|e| DatabaseError::deserialise_payload(e.to_string(), "Cannot read new_version column".into()))?;
                let version = row
                    .try_get::<&str, Option<i64>>("version")
                    .map_err(|e| DatabaseError::deserialise_payload(e.to_string(), "Cannot read version column".into()))?;
                let snapshot = row
                    .try_get::<&str, i64>("snapshot")
                    .map_err(|e| DatabaseError::deserialise_payload(e.to_string(), "Cannot read snapshot column".into()))?;

                Ok((nr, new_ver, version, snapshot))
            })
            .await
            .map_err(|e| e.to_string())?;

        if result.is_empty() {
            // there were no items found to work with
            log::warn!("No bank accounts where found by these IDs: {:?}", (request.from.clone(), request.to.clone()));
            let c = Arc::clone(&self.counter_oo_no_data_found);
            tokio::spawn(async move {
                c.add(1, &[]);
            });
            return Ok(OutOfOrderInstallOutcome::InstalledAlready);
        }

        // Quickly grab the snapshot to check whether safepoint condition is satisfied. Any row can be used for that.
        let (_, _, _, snapshot) = result[0];
        if (snapshot as u64) < safepoint {
            return Ok(OutOfOrderInstallOutcome::SafepointCondition);
        }

        // Now we know that it was safe to execute install. We either just installed or we were late and replicator has done it before us.
        // The number of returned rows could be anything from 1 to 4.
        // When:
        //      1:  Edge case. We wanted to update 2 accounts, but one account got deleted from DB, thats why "SELECT ... WHERE number IN($1,$2)" could find only one account.
        //          With that single account which we found, we or replicator did the installation.
        //      4:  Happy path. We updated two accounts and also queried them using the bottom part of UNION statement.
        //      2:  This is possible in two scenarios.
        //          2.1:    Happy path. We could not update anyhting, so we just queried data using the bottom part of UNION statement. Replicator has done the work.
        //          2.2:    Edge case. We could not find one account because it was deleted. This returned only one row: "SELECT ... WHERE number IN($1,$2)".
        //                  However, that rows was returned 2 times, one time by each arm of UNION. Basically this is the same as case "4" but applied to one account only.
        //      3:  Only one accout was updated by us, and two accouts were queried by bottom part of UNION statement, while another accout has been updated by replicator.

        // Code below is for debugging purposes only
        if self.detailed_logging {
            if result.len() == 1 {
                let (number, new_ver, version, _snapshot) = &result[0];
                if new_ver.is_none() {
                    log::debug!(
                        "Case 1: No rows were updated for xid '{}' when installing out of order data with new version {}. Account {} version is now {:?}. Another candidate account was not found",
                        xid,
                        new_version,
                        number,
                        version,
                    );
                } else {
                    log::debug!(
                        "Case 1: 1 row was updated for xid '{}' when installing out of order data with new version {}. Account {} version is now {:?}. Another candidate account was not found",
                        xid,
                        new_version,
                        number,
                        new_ver,
                    );
                }
            } else if result.len() == 2 {
                let accounts: HashSet<&String> = result.iter().map(|(n, _, _, _)| n).collect();
                if accounts.len() == 2 {
                    // 2.1
                    let (_, _, version_from, _) = &result[0];
                    let (_, _, version_to, _) = &result[1];
                    log::debug!(
                        "Case 2.1: No rows were updated for xid '{}' when installing out of order data with new version {}. Current versions have moved to {:?}",
                        xid,
                        new_version,
                        (version_from, version_to)
                    );
                } else {
                    // 2.2
                    let (number, new_ver, _, _) = &result[0];
                    log::debug!(
                        "Case 2.2: 1 row was updated for xid '{}' when installing out of order data with new version {}. Account {} version is now {:?}. Another candidate account was not found",
                        xid,
                        new_version,
                        number,
                        new_ver,
                    );
                }
            } else if result.len() == 3 {
                let (number, new_ver, _, _) = &result[0];
                // Since order of rows in the UNION bottom arm is not known, we do a simple comparison to find row correspinding to 'another' account (the one we did not update).
                let (number_a, _, _, _) = &result[1];
                let (_, _, version, _) = if *number_a == *number { &result[2] } else { &result[1] };

                log::debug!(
                    "Case 3: 1 row was updated for xid '{}' when installing out of order data with new version {}. Account {} version is now {:?}. Another accout was already set to: {:?}",
                    xid,
                    new_version,
                    number,
                    new_ver,
                    version,
                );
            }
        }

        Ok(OutOfOrderInstallOutcome::Installed)
    }
    pub async fn install_statemap(&self, install_item: OutOfOrderInstallRequest) -> Result<OutOfOrderInstallOutcome, String> {
        // TODO: GK -
        // For our testing the statemap size is 1,so the below approach is fine. But if we have to install multiple statemaps, then we would ideally
        // use a transaction and either install all or none, depending on error and handle the return accordingly.
        for statemap in install_item.statemaps.iter() {
            let payload = statemap.values().next().unwrap();
            let request: TransferRequest = serde_json::from_value(payload.clone()).unwrap();
            let result = self
                .install_using_single_query(&install_item.xid, install_item.safepoint, install_item.version, &request)
                .await?;

            if result != OutOfOrderInstallOutcome::Installed {
                return Ok(result);
            };
        }

        Ok(OutOfOrderInstallOutcome::Installed)
    }
}

#[async_trait]
impl OutOfOrderInstaller for OutOfOrderInstallerImpl {
    async fn install(&self, install_item: OutOfOrderInstallRequest) -> Result<OutOfOrderInstallOutcome, String> {
        self.install_statemap(install_item).await
    }
}
