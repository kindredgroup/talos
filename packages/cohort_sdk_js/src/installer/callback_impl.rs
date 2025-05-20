use async_trait::async_trait;
use napi::{bindgen_prelude::Promise, threadsafe_function::ThreadsafeFunction};
use talos_cohort_replicator::{
    callbacks::{ReplicatorInstaller, ReplicatorSnapshotProvider},
    StatemapItem,
};
use tokio::time::Instant;
use tracing::info;

use super::JsStatemapAndSnapshot;

pub struct SnapshotProviderDelegate {
    pub(crate) callback: ThreadsafeFunction<()>,
    pub(crate) update_snapshot_callback: ThreadsafeFunction<i64>,
}

#[async_trait]
impl ReplicatorSnapshotProvider for SnapshotProviderDelegate {
    async fn get_snapshot(&self) -> Result<u64, String> {
        let result = self.callback.call_async::<Promise<i64>>(Ok(())).await;

        match result {
            Ok(promise) => promise
                .await
                .map(|v| v as u64)
                // Here reason is empty with NAPI 2.10.3
                .map_err(|e| format!("Unable to retrieve snapshot. Native reason reported from JS: \"{}\"", e.reason)),

            Err(e) => Err(e.to_string()),
        }
    }
    async fn update_snapshot(&self, version: u64) -> Result<(), String> {
        let result = self.update_snapshot_callback.call_async::<Promise<()>>(Ok(version as i64)).await;

        match result {
            Ok(promise) => promise
                .await
                .map(|_| ())
                .map_err(|e| format!("Updating snapshot to version {version} failed with error {}", e.reason)),
            Err(e) => Err(e.to_string()),
        }
    }
}

pub struct StatemapInstallerDelegate {
    pub(crate) callback: ThreadsafeFunction<JsStatemapAndSnapshot>,
}

#[async_trait]
impl ReplicatorInstaller for StatemapInstallerDelegate {
    async fn install(&self, sm: Vec<StatemapItem>, version: u64) -> Result<(), String> {
        let data = JsStatemapAndSnapshot {
            statemap: sm.iter().map(|i| (*i).clone().into()).collect(),
            version: version as i64,
        };

        let start = Instant::now();
        let result = self.callback.call_async::<Promise<()>>(Ok(data)).await;
        let end_call_async = start.elapsed().as_millis();

        let start_2 = Instant::now();
        let result = match result {
            Ok(promise) => promise
                .await
                // Here reason is empty with NAPI 2.10.3
                .map_err(|e| format!("Unable to install statemap. Native reason reported from JS: \"{}\"", e.reason)),

            Err(e) => Err(e.to_string()),
        };

        info!(
            "[Version<{version}] - RUST install callback timing - Total time = {}ms | Time taken for call_async = {end_call_async}ms | Time taken for promise resolve = {}ms ",
            start.elapsed().as_millis(),
            start_2.elapsed().as_millis(),
        );
        result
    }
}
