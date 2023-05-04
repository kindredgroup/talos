use std::str::FromStr;

use crate::actions::account_update::AccountUpdate;
use crate::actions::transfer::Transfer;
use crate::replicator::core::StatemapItem;
use crate::snapshot_api::SnapshotApi;
use crate::state::model::{AccountUpdateRequest, BusinessActionType, TransferRequest};
use crate::state::postgres::database::{Action, Database};
use deadpool_postgres::GenericClient;
use futures::future::BoxFuture;

pub struct BatchExecutor {}

impl BatchExecutor {
    pub async fn execute(db: &Database, batch: Vec<StatemapItem>, snapshot: Option<u64>) -> Result<u64, String> {
        //
        // We attempt to execute all actions in this batch and then track how many DB rows where affected.
        // If there were no rows updated in DB then we print warning and allow cohort to proceed.
        // In case of batch execution produced an error we rollback.
        // If rollback fails we return error describing both - the reson for batch execution error and the reason for rollback error.
        // If successfull we check whether snapshot update is required.
        // Then we udpate snapshot and commit. Or we update snapshot, fail and rollback.
        // The error handling of commit is the same as for rollabck error.

        let mut client = db.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();

        let mut batch_async: Vec<BoxFuture<Result<Option<u64>, String>>> = Vec::new();
        for item in batch.iter() {
            let pinned_box: BoxFuture<Result<Option<u64>, String>> = Box::pin(Self::execute_item(item, &tx));
            batch_async.push(pinned_box);
        }

        let action_result = futures::future::try_join_all(batch_async).await;
        if action_result.is_err() {
            return Err(Self::handle_rollback(tx.rollback().await, action_result.unwrap_err()));
        }

        let mut affected_rows = 0_u64;
        // filter out empty Option elements, here flatten() = filter(Option::is_some).map(Option::unwrap)
        for c in action_result.unwrap().iter().flatten() {
            affected_rows += c;
        }

        if let Some(new_version) = snapshot {
            if affected_rows == 0 {
                log::warn!("No rows were updated when executing batch. Snapshot will be set to: {}", new_version);
            }

            let snapshot_update_result = SnapshotApi::update_using(&tx, new_version).await;
            if let Ok(rows) = snapshot_update_result {
                affected_rows += rows;
            } else {
                // there was an error updating snapshot, we need to rollabck the whole batch
                let snapshot_error = snapshot_update_result.unwrap_err();
                return Err(Self::handle_rollback(
                    tx.rollback().await,
                    format!("Snpshot update error: '{}'", snapshot_error),
                ));
            }
        } else if affected_rows == 0 {
            log::warn!("No rows were updated when executing batch.");
        }

        tx.commit().await.map_err(|tx_error| format!("Commit error: {}", tx_error))?;
        Ok(affected_rows)
    }

    async fn execute_item<T>(item: &StatemapItem, client: &T) -> Result<Option<u64>, String>
    where
        T: GenericClient + Sync,
    {
        let rslt_parse_type = BusinessActionType::from_str(&item.action);
        if let Err(e) = rslt_parse_type {
            // This case is expected on the cohort where some business actions are not implemented.
            // Another way to implement this is to create custom to/from string for BusinessActionType and
            // map unkown values into "catch all" enum option "BusinessActionType::UNIMPLEMENTED(raw: String)".
            log::warn!("Unknown action type in statemap item: '{}'. Skipping with parser error: {}", item.action, e);
            return Ok(None);
        }

        let action_outcome = match rslt_parse_type.unwrap() {
            BusinessActionType::TRANSFER => {
                let data: TransferRequest = serde_json::from_value(item.payload.clone()).map_err(|e| e.to_string())?;
                Some(Transfer::new(data.from, data.to, data.amount, item.version).execute(client).await?)
            }
            BusinessActionType::DEPOSIT => {
                let data: AccountUpdateRequest = serde_json::from_value(item.payload.clone()).map_err(|e| e.to_string())?;
                Some(AccountUpdate::deposit(data, item.version).execute(client).await?)
            }
            BusinessActionType::WITHDRAW => {
                let data: AccountUpdateRequest = serde_json::from_value(item.payload.clone()).map_err(|e| e.to_string())?;
                Some(AccountUpdate::withdraw(data, item.version).execute(client).await?)
            }
        };

        Ok(action_outcome)
    }

    fn handle_rollback(tx_res: Result<(), tokio_postgres::Error>, context_error: String) -> String {
        if let Err(tx_error) = tx_res {
            format!("Cannot rollback failed action. Error: ${:?}. Rollback error: {}", context_error, tx_error)
        } else {
            format!("Cannot execute action. Error: ${:?}", context_error)
        }
    }
}
