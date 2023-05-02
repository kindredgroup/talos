use std::str::FromStr;

use crate::bank_api::{AccountUpdate, Transfer};
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
        // If there were no rows updated in DB then we treat this as error.
        // In case of batch execution produced an error we rollback.
        // If rollback fails we return error describing both - the reson for batch execution error and the reason for rollback error.
        // If successfull we check whether snapshot update is required.
        // Then we udpate snapshot and commit. Or we update snapshot, fail and rollback.
        // The error handling of commit is the same as for rollabck error.

        let mut client = db.pool.get().await.unwrap();
        let tx = client.transaction().await.unwrap();

        let mut batch_async: Vec<BoxFuture<Result<u64, String>>> = Vec::new();
        let last_item_nr = batch.len() - 1;
        for (index, item) in batch.iter().enumerate() {
            let update_version = index == last_item_nr;
            let pinned_box: BoxFuture<Result<u64, String>> = Box::pin(Self::execute_item(item, &tx, update_version));
            batch_async.push(pinned_box);
        }

        let action_result = futures::future::try_join_all(batch_async).await;

        if action_result.is_err() {
            let action_error = action_result.unwrap_err();
            let tx_error = tx.rollback().await.map_err(|e| e.to_string());

            if tx_error.is_err() {
                Err(format!(
                    "Cannot rollback failed action. Error: {:?}, Rollback error: {}",
                    action_error,
                    tx_error.unwrap_err(),
                ))
            } else {
                Err(format!("Cannot execute batch. Error: {:?}. Rollback.", action_error))
            }
        } else {
            let mut affected_rows = 0_u64;
            for c in action_result.unwrap().iter() {
                affected_rows += c;
            }

            if affected_rows == 0 {
                // still fail here
                let tx_error = tx.rollback().await.map_err(|e| e.to_string());
                if tx_error.is_err() {
                    Err(format!(
                        "Cannot rollback failed action. Error: No rows where updated. Rollback error: {}",
                        tx_error.unwrap_err(),
                    ))
                } else {
                    Err("Cannot execute action. Error: No rows where updated".to_string())
                }
            } else {
                //
                if let Some(new_version) = snapshot {
                    let snapshot_update_result = SnapshotApi::update_using(&tx, new_version).await;
                    if let Ok(rows) = snapshot_update_result {
                        affected_rows += rows;
                    } else {
                        let snapshot_error = snapshot_update_result.unwrap_err();
                        // there was an error updating snapshot, we need to rollabck the whole batch
                        let tx_error = tx.rollback().await.map_err(|e| e.to_string());
                        return if tx_error.is_err() {
                            Err(format!(
                                "Cannot rollback after snapshot failed to update. Error: {}. Rollback error: {}",
                                snapshot_error,
                                tx_error.unwrap_err(),
                            ))
                        } else {
                            Err(format!("Cannot update snapshot. Error: {}. Rollback", snapshot_error))
                        };
                    }
                }

                tx.commit().await.map_err(|tx_error| format!("Commit error: {}", tx_error))?;
                Ok(affected_rows)
            }
        }
    }

    async fn execute_item<T>(item: &StatemapItem, client: &T, update_item_version: bool) -> Result<u64, String>
    where
        T: GenericClient + Sync,
    {
        // TODO: Do not fail on unknown actions, print warning
        let action_type: BusinessActionType = BusinessActionType::from_str(&item.action)
            .map_err(|e| format!("Unable to parse BusinessActionType. UnknownValue: {}. Error: {}", &item.action, e))
            .unwrap();
        match action_type {
            BusinessActionType::TRANSFER => {
                let data: TransferRequest = serde_json::from_value(item.payload.clone()).map_err(|e| e.to_string())?;
                Transfer::new(data.from, data.to, data.amount, item.version, update_item_version)
                    .execute(client)
                    .await
            }
            BusinessActionType::DEPOSIT => {
                let data: AccountUpdateRequest = serde_json::from_value(item.payload.clone()).map_err(|e| e.to_string())?;
                AccountUpdate::deposit(data, item.version, update_item_version).execute(client).await
            }
            BusinessActionType::WITHDRAW => {
                let data: AccountUpdateRequest = serde_json::from_value(item.payload.clone()).map_err(|e| e.to_string())?;
                AccountUpdate::withdraw(data, item.version, update_item_version).execute(client).await
            }
        }
    }
}
