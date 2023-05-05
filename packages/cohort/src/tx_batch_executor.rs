use std::str::FromStr;

use crate::actions::account_update::AccountUpdate;
use crate::actions::transfer::Transfer;
use crate::model::requests::{AccountUpdateRequest, BusinessActionType, TransferRequest};
use crate::replicator::core::StatemapItem;
use crate::snapshot_api::SnapshotApi;
use crate::state::data_access_api::{ManualTx, TxApi};
use crate::state::postgres::database::Action;
use futures::future::BoxFuture;

pub struct BatchExecutor {}

impl BatchExecutor {
    pub async fn execute<'a, T, A>(manual_tx_api: &'a mut A, batch: Vec<StatemapItem>, snapshot: Option<u64>) -> Result<u64, String>
    where
        T: ManualTx,
        A: TxApi<'a, T>,
    {
        //
        // We attempt to execute all actions in this batch and then track how many DB rows where affected.
        // If there were no rows updated in DB then we print warning and allow cohort to proceed.
        // In case of batch execution produced an error we rollback.
        // If rollback fails we return error describing both - the reson for batch execution error and the reason for rollback error.
        // If successfull we check whether snapshot update is required.
        // Then we udpate snapshot and commit. Or we update snapshot, fail and rollback.
        // The error handling of commit is the same as for rollabck error.

        let tx = manual_tx_api.transaction().await;

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
        T: ManualTx,
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
                Some(Transfer::new(data, item.version).execute(client).await?)
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

    fn handle_rollback(tx_res: Result<(), String>, context_error: String) -> String {
        if let Err(tx_error) = tx_res {
            format!("Cannot rollback failed action. Error: {:?}. Rollback error: {}", context_error, tx_error)
        } else {
            format!("Cannot execute action. Error: {:?}", context_error)
        }
    }
}

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use crate::snapshot_api::SNAPSHOT_UPDATE_QUERY;

    use super::*;

    use async_trait::async_trait;
    use mockall::{mock, Sequence};
    use serde::Serialize;
    use tokio_postgres::types::ToSql;

    mock! {
        Tx {}
        #[async_trait]
        impl ManualTx for Tx {
            pub async fn commit(self) -> Result<(), String>;
            pub async fn rollback(self) -> Result<(), String>;
            pub async fn execute<'a, 'b, 'c>(&'a self, sql: String, params: &'b [&'c (dyn ToSql + Sync + 'c)]) -> Result<u64, String>;
        }
    }

    mock! {
        TxProvider {}
        #[async_trait]
        impl<'a> TxApi<'a, MockTx> for TxProvider {
            async fn transaction(&'a mut self) -> MockTx;
        }
    }

    fn expect_opt_rows(result: Result<Option<u64>, String>, rows: u64) {
        if let Err(e) = result {
            assert_eq!("no errors are expected", e);
        } else {
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), Some(rows));
        }
    }

    fn expect_rows(result: Result<u64, String>, rows: u64) {
        if let Err(e) = result {
            assert_eq!("no errors are expected", e);
        } else {
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), rows);
        }
    }

    fn item<T: Serialize>(action: &str, version: u64, payload: T) -> StatemapItem {
        StatemapItem::new(action.to_string(), version, serde_json::to_value(payload).unwrap())
    }

    #[tokio::test]
    async fn excute_item_should_recognise_deposit() {
        let mut tx = MockTx::new();
        tx.expect_commit().never();
        tx.expect_rollback().never();
        tx.expect_execute().withf(move |_, params| params.len() == 3).returning(move |_, _| Ok(1));
        let item = item("DEPOSIT", 1, AccountUpdateRequest::new("a1".into(), "10.0".into()));
        let result = BatchExecutor::execute_item(&item, &tx).await;
        expect_opt_rows(result, 1);
    }

    #[tokio::test]
    async fn excute_item_should_recognise_withdraw() {
        let mut tx = MockTx::new();
        tx.expect_commit().never();
        tx.expect_rollback().never();
        tx.expect_execute().withf(move |_, params| params.len() == 3).returning(move |_, _| Ok(1));
        let item = item("WITHDRAW", 1, AccountUpdateRequest::new("a1".into(), "10.0".into()));
        let result = BatchExecutor::execute_item(&item, &tx).await;
        expect_opt_rows(result, 1);
    }

    #[tokio::test]
    async fn excute_item_should_recognise_transfer() {
        let mut tx = MockTx::new();
        tx.expect_commit().never();
        tx.expect_rollback().never();
        tx.expect_execute().withf(move |_, params| params.len() == 5).returning(move |_, _| Ok(1));
        let item = item("TRANSFER", 1, TransferRequest::new("a1".into(), "a2".into(), "10.0".into()));
        let result = BatchExecutor::execute_item(&item, &tx).await;
        expect_opt_rows(result, 1);
    }

    #[tokio::test]
    async fn excute_item_should_return_none_when_action_is_unknown() {
        let mut tx = MockTx::new();
        tx.expect_commit().never();
        tx.expect_rollback().never();
        tx.expect_execute().never();
        let item = item("NO SUCH ACTION", 1, 1);
        let result = BatchExecutor::execute_item(&item, &tx).await;
        if let Err(e) = result {
            assert_eq!("no errors are expected", e);
        } else {
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), None);
        }
    }

    // Below are various test scenarios to cover batch execution logic and handling of transaction

    #[tokio::test]
    async fn should_update_snapshot() {
        let mut tx = MockTx::new();

        // 1st and 2nd for items plus 3rd for the snapshot

        let mut seq = Sequence::new();
        tx.expect_rollback().never();
        tx.expect_execute().times(2).returning(move |_, _| Ok(1)).in_sequence(&mut seq);

        tx.expect_execute()
            .withf(move |sql, _| sql.eq(SNAPSHOT_UPDATE_QUERY))
            .once()
            .returning(move |_, _| Ok(1))
            .in_sequence(&mut seq);

        tx.expect_commit().once().returning(move || Ok(()));

        let item1 = item("DEPOSIT", 1, AccountUpdateRequest::new("a1".into(), "10.0".into()));
        let item2 = item("DEPOSIT", 1, AccountUpdateRequest::new("a1".into(), "10.0".into()));

        let mut cnn = MockTxProvider::new();
        cnn.expect_transaction().return_once(move || tx);

        let result = BatchExecutor::execute(&mut cnn, vec![item1, item2], Some(1)).await;
        expect_rows(result, 3);
    }

    #[tokio::test]
    async fn should_not_update_snapshot() {
        let mut tx = MockTx::new();
        // 1st and 2nd for items plus 3rd for the snapshot

        tx.expect_execute().times(2).returning(move |_, _| Ok(1));
        tx.expect_execute().withf(move |sql, _| sql.eq(SNAPSHOT_UPDATE_QUERY)).never();
        tx.expect_commit().once().returning(move || Ok(()));
        tx.expect_rollback().never();

        let item1 = item("DEPOSIT", 1, AccountUpdateRequest::new("a1".into(), "10.0".into()));
        let item2 = item("DEPOSIT", 1, AccountUpdateRequest::new("a1".into(), "10.0".into()));

        let mut cnn = MockTxProvider::new();
        cnn.expect_transaction().return_once(move || tx);

        let result = BatchExecutor::execute(&mut cnn, vec![item1, item2], None).await;
        expect_rows(result, 2);
    }

    #[tokio::test]
    async fn should_rollback_on_snapshot_failure() {
        let mut tx = MockTx::new();
        tx.expect_execute().times(2).returning(move |_, _| Ok(1));
        tx.expect_execute()
            .withf(move |sql, _| sql.eq(SNAPSHOT_UPDATE_QUERY))
            .once()
            .returning(move |_, _| Err("cannot udpate snapshot".into()));
        tx.expect_rollback().once().returning(move || Ok(()));
        tx.expect_commit().never();

        let item1 = item("DEPOSIT", 1, AccountUpdateRequest::new("a1".into(), "10.0".into()));
        let item2 = item("DEPOSIT", 1, AccountUpdateRequest::new("a1".into(), "10.0".into()));

        let mut cnn = MockTxProvider::new();
        cnn.expect_transaction().return_once(move || tx);

        let result = BatchExecutor::execute(&mut cnn, vec![item1, item2], Some(1)).await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Cannot execute action. Error: \"Snpshot update error: 'cannot udpate snapshot'\"",
        );
    }

    #[tokio::test]
    async fn should_rollback_on_action_failure() {
        let mut tx = MockTx::new();
        let mut seq = Sequence::new();

        tx.expect_execute().once().returning(move |_, _| Ok(1)).in_sequence(&mut seq);

        tx.expect_execute()
            .once()
            .returning(move |_, _| Err("Network problem".into()))
            .in_sequence(&mut seq);

        tx.expect_execute().withf(move |sql, _| sql.eq(SNAPSHOT_UPDATE_QUERY)).never();
        tx.expect_commit().never();
        tx.expect_rollback().once().returning(move || Ok(()));

        let item1 = item("DEPOSIT", 1, AccountUpdateRequest::new("a1".into(), "10.0".into()));
        let item2 = item("DEPOSIT", 1, AccountUpdateRequest::new("a1".into(), "10.0".into()));

        let mut cnn = MockTxProvider::new();
        cnn.expect_transaction().return_once(move || tx);

        let result = BatchExecutor::execute(&mut cnn, vec![item1, item2], None).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Cannot execute action. Error: \"Network problem\"");
    }

    #[tokio::test]
    async fn should_not_fail_if_replicator_run_faster_with_snapshot() {
        let mut tx = MockTx::new();

        tx.expect_execute().times(3).returning(move |_, _| Ok(0));
        tx.expect_commit().once().returning(move || Ok(()));
        tx.expect_rollback().never();

        let item1 = item("DEPOSIT", 1, AccountUpdateRequest::new("a1".into(), "10.0".into()));
        let item2 = item("DEPOSIT", 1, AccountUpdateRequest::new("a1".into(), "10.0".into()));

        let mut cnn = MockTxProvider::new();
        cnn.expect_transaction().return_once(move || tx);

        let result = BatchExecutor::execute(&mut cnn, vec![item1, item2], Some(1)).await;
        expect_rows(result, 0);
    }

    #[tokio::test]
    async fn should_not_fail_if_replicator_run_faster_without_snapshot() {
        let mut tx = MockTx::new();

        tx.expect_execute().times(2).returning(move |_, _| Ok(0));
        tx.expect_commit().once().returning(move || Ok(()));
        tx.expect_rollback().never();

        let item1 = item("DEPOSIT", 1, AccountUpdateRequest::new("a1".into(), "10.0".into()));
        let item2 = item("DEPOSIT", 1, AccountUpdateRequest::new("a1".into(), "10.0".into()));

        let mut cnn = MockTxProvider::new();
        cnn.expect_transaction().return_once(move || tx);

        let result = BatchExecutor::execute(&mut cnn, vec![item1, item2], None).await;
        expect_rows(result, 0);
    }

    #[tokio::test]
    async fn should_not_fail_if_rollback_was_not_possible() {
        let mut tx = MockTx::new();
        tx.expect_execute().once().returning(move |_, _| Err("Network problem".into()));
        tx.expect_commit().never();
        tx.expect_rollback().once().returning(move || Err("Network problem on rollback".into()));

        let item1 = item("DEPOSIT", 1, AccountUpdateRequest::new("a1".into(), "10.0".into()));
        let item2 = item("DEPOSIT", 1, AccountUpdateRequest::new("a1".into(), "10.0".into()));

        let mut cnn = MockTxProvider::new();
        cnn.expect_transaction().return_once(move || tx);

        let result = BatchExecutor::execute(&mut cnn, vec![item1, item2], None).await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Cannot rollback failed action. Error: \"Network problem\". Rollback error: Network problem on rollback",
        );
    }
}
// $coverage:ignore-end
