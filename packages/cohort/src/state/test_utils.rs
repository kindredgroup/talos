// $coverage:ignore-start
use crate::state::model::{AccountOperation, Envelope, OperationResponse};
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

pub async fn mock_state_manager<F>(
    expected_receive: AccountOperation,
    fn_replier: F,
) -> (Sender<Envelope<AccountOperation, OperationResponse>>, JoinHandle<String>)
where
    F: Fn(tokio::sync::oneshot::Sender<OperationResponse>) -> String + Send + Sync + 'static,
{
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Envelope<AccountOperation, OperationResponse>>(10);
    let handle = tokio::task::spawn(async move {
        let data = rx.recv().await;
        if data.is_none() {
            return "No data received".to_string();
        }
        let envelope = data.unwrap();
        if envelope.data != expected_receive {
            return format!("Invalid operation {:?}, ${:?} is expected", envelope.data, expected_receive);
        }

        fn_replier(envelope.tx_reply)
    });

    (tx, handle)
}

pub fn mock_with_closed_receiver() -> Sender<Envelope<AccountOperation, OperationResponse>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Envelope<AccountOperation, OperationResponse>>(10);
    drop(rx);
    tx
}

pub async fn mock_response_success(operation: &AccountOperation) -> (Sender<Envelope<AccountOperation, OperationResponse>>, JoinHandle<String>) {
    let (tx, handle) = mock_state_manager(operation.clone(), |tx| {
        let _ = tx.send(OperationResponse::Success);
        "Success".to_string()
    })
    .await;
    (tx, handle)
}

pub async fn mock_response_empty_query(operation: &AccountOperation) -> (Sender<Envelope<AccountOperation, OperationResponse>>, JoinHandle<String>) {
    let (tx, handle) = mock_state_manager(operation.clone(), |tx| {
        let _ = tx.send(OperationResponse::QueryResult(None));
        "Success".to_string()
    })
    .await;
    (tx, handle)
}

pub async fn mock_resp_wrong_account(operation: &AccountOperation) -> (Sender<Envelope<AccountOperation, OperationResponse>>, JoinHandle<String>) {
    let (tx, handle) = mock_state_manager(operation.clone(), |tx| {
        let _ = tx.send(OperationResponse::Error("wrong account".to_string()));
        "Success".to_string()
    })
    .await;
    (tx, handle)
}

pub async fn mock_with_closed_reply_channel(operation: &AccountOperation) -> (Sender<Envelope<AccountOperation, OperationResponse>>, JoinHandle<String>) {
    let (tx, handle) = mock_state_manager(operation.clone(), |tx| {
        drop(tx);
        "Success".to_string()
    })
    .await;
    (tx, handle)
}

pub fn expect_error<T>(response: Result<T, String>, error: &str) {
    let expected_error = if let Err(e) = response {
        if e.contains(error) {
            true
        } else {
            log::info!("Expected: '{}', got: '{}'", error, e);
            false
        }
    } else {
        false
    };
    assert!(expected_error);
}
// $coverage:ignore-end
