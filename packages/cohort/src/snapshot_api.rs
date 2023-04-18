use crate::state::model::{AccountOperation, Envelope, OperationResponse, Snapshot};
use tokio::sync::mpsc::Sender;

pub struct SnapshotApi {}

impl SnapshotApi {
    pub async fn query(tx_state: Sender<Envelope<AccountOperation, OperationResponse>>) -> Result<Snapshot, String> {
        let (tx, rx) = tokio::sync::oneshot::channel::<OperationResponse>();
        let resp = tx_state.send(Envelope::new(AccountOperation::QuerySnapshot, tx)).await;

        if let Err(se) = resp {
            return Err(format!("Internal error requesting snapshot operation: {}", se));
        }
        let rslt = rx.await;
        if let Err(re) = rslt {
            return Err(format!("Internal error waiting for answer: {}", re));
        }

        match rslt.unwrap() {
            OperationResponse::Snapshot(snapshot) => Ok(snapshot),
            OperationResponse::Error(e) => Err(format!("Error requesting snapshot value: {}", e)),
            OperationResponse::QueryResult(_) => Err("Unexpected response 'QueryResult' to QuerySnapshot".to_string()),
            OperationResponse::Success => Err("Unexpected response 'Success' to QuerySnapshot".to_string()),
        }
    }

    pub async fn update(tx_state: Sender<Envelope<AccountOperation, OperationResponse>>, new_version: u64) -> Result<(), String> {
        let (tx, rx) = tokio::sync::oneshot::channel::<OperationResponse>();
        let resp = tx_state.send(Envelope::new(AccountOperation::UpdateSnapshot(new_version), tx)).await;

        if let Err(se) = resp {
            return Err(format!("Internal error requesting snapshot operation: {}", se));
        }
        let rslt = rx.await;
        if let Err(re) = rslt {
            return Err(format!("Internal error waiting for answer: {}", re));
        }

        match rslt.unwrap() {
            OperationResponse::Success => Ok(()),
            OperationResponse::Error(e) => Err(format!("Error requesting snapshot value: {}", e)),
            OperationResponse::QueryResult(_) => Err("Unexpected response 'QueryResult' to UpdateSnapshot".to_string()),
            OperationResponse::Snapshot(_) => Err("Unexpected response 'Snapshot' to UpdateSnapshot".to_string()),
        }
    }
}

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::test_utils::{expect_error, mock_response_success, mock_state_manager, mock_with_closed_receiver, mock_with_closed_reply_channel};
    use std::assert_eq;

    #[tokio::test]
    async fn update() {
        let (tx, handle) = mock_state_manager(AccountOperation::UpdateSnapshot(10), |tx| {
            let _ = tx.send(OperationResponse::Success);
            "Success".to_string()
        })
        .await;
        let resp = SnapshotApi::update(tx, 10).await;
        assert!(resp.is_ok());
        assert_eq!("Success", handle.await.unwrap());
    }

    #[tokio::test]
    async fn update_should_fail() {
        // Simulate state manager replying bad account...
        let operation = AccountOperation::UpdateSnapshot(10);
        let resp = SnapshotApi::update(mock_with_closed_receiver(), 10).await;
        expect_error(resp, "Internal error requesting snapshot operation: channel closed");

        let (tx, handle) = mock_with_closed_reply_channel(&operation).await;
        let resp = SnapshotApi::update(tx, 10).await;
        expect_error(resp, "channel closed");
        assert_eq!("Success", handle.await.unwrap());

        let (tx, handle) = mock_response_success(&operation).await;
        let resp = SnapshotApi::update(tx, 10).await;
        assert!(resp.is_ok());
        assert_eq!("Success", handle.await.unwrap());

        let (tx, handle) = mock_state_manager(operation.clone(), |tx| {
            let _ = tx.send(OperationResponse::Snapshot(Snapshot::from(11)));
            "Success".to_string()
        })
        .await;

        let resp = SnapshotApi::update(tx, 10).await;
        expect_error(resp, "Unexpected response 'Snapshot'");
        assert_eq!("Success", handle.await.unwrap());

        let (tx, handle) = mock_state_manager(operation.clone(), |tx| {
            let _ = tx.send(OperationResponse::Error("some-error".to_string()));
            "Success".to_string()
        })
        .await;

        let resp = SnapshotApi::update(tx, 10).await;
        expect_error(resp, "Error requesting snapshot value: some-error");
        assert_eq!("Success", handle.await.unwrap());

        let (tx, handle) = mock_state_manager(operation.clone(), |tx| {
            let _ = tx.send(OperationResponse::QueryResult(None));
            "Success".to_string()
        })
        .await;

        let resp = SnapshotApi::update(tx, 10).await;
        expect_error(resp, "Unexpected response 'QueryResult'");
        assert_eq!("Success", handle.await.unwrap());
    }
}
// $coverage:ignore-end
