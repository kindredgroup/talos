use futures::Future;

/// Callback fn used in the `installer_queue_service` to retrieve the current snapshot.
pub async fn get_snapshot_callback(callback_fn: impl Future<Output = Result<u64, String>>) -> Result<u64, String> {
    let snapshot = callback_fn.await?;
    Ok(snapshot)
}
