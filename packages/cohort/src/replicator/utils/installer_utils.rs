use futures::Future;

use crate::model::snapshot::Snapshot;

/// Callback fn used in the `installer_queue_service` to retrieve the current snapshot.
pub async fn get_snapshot_callback(callback_fn: impl Future<Output = Result<Snapshot, String>>) -> Result<u64, String> {
    let snapshot = callback_fn.await?;
    Ok(snapshot.version)
}
