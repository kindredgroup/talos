use async_trait::async_trait;
use napi::{bindgen_prelude::Promise, threadsafe_function::ThreadsafeFunction};
use talos_cohort_replicator::{
    callbacks::{ReplicatorInstaller, ReplicatorSnapshotProvider},
    StatemapItem,
};

use crate::map_error_to_napi_error;

use super::JsStatemapAndSnapshot;

pub struct SnapshotProviderDelegate {
    pub(crate) callback: ThreadsafeFunction<()>,
}

#[async_trait]
impl ReplicatorSnapshotProvider for SnapshotProviderDelegate {
    async fn get_snapshot(&self) -> Result<u64, String> {
        let result = self.callback.call_async::<Promise<i64>>(Ok(())).await.map_err(map_error_to_napi_error);

        match result {
            Ok(promise) => promise.await.map(|v| v as u64).map_err(|e| e.to_string()),

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

        let result = self.callback.call_async::<Promise<()>>(Ok(data)).await.map_err(map_error_to_napi_error);

        match result {
            Ok(promise) => promise.await.map_err(|e| e.to_string()),

            Err(e) => Err(e.to_string()),
        }
    }
}
