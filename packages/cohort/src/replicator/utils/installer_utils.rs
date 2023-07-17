use ahash::RandomState;
use futures::Future;
use indexmap::IndexMap;

use crate::{
    model::snapshot::Snapshot,
    replicator::core::{StatemapInstallState, StatemapInstallerHashmap},
};

/// Callback fn used in the `installer_queue_service` to retrieve the current snapshot.
pub async fn get_snapshot_callback(callback_fn: impl Future<Output = Result<Snapshot, String>>) -> Result<u64, String> {
    let snapshot = callback_fn.await?;
    Ok(snapshot.version)
}

#[derive(Debug, Default)]
pub struct StatemapInstallerQueue {
    pub queue: IndexMap<u64, StatemapInstallerHashmap, RandomState>,
    pub snapshot_version: u64,
}

impl StatemapInstallerQueue {
    pub fn update_snapshot(&mut self, snapshot_version: u64) {
        self.snapshot_version = snapshot_version;
    }

    pub fn insert_queue_item(&mut self, key: &u64, value: StatemapInstallerHashmap) {
        self.queue.insert(*key, value);
    }

    pub fn update_queue_item_state(&mut self, key: &u64, state: StatemapInstallState) {
        if let Some(item) = self.queue.get_mut(key) {
            item.state = state
        };
    }

    pub fn remove_installed(&mut self) -> Option<u64> {
        let Some(index) = self.queue.get_index_of(&self.snapshot_version) else { return None;};

        let items = self.queue.drain(..index);

        Some(items.count() as u64)
    }

    /// Filter items in queue based on the `StatemapInstallState`
    pub fn filter_items_by_state(&self, state: StatemapInstallState) -> impl Iterator<Item = &StatemapInstallerHashmap> {
        self.queue.values().filter(move |&x| x.state == state)
    }

    pub fn get_versions_to_install(&self) -> Vec<u64> {
        self
            // Get items in awaiting
            .filter_items_by_state(StatemapInstallState::Awaiting)
            // Get items whose safepoint is below the snapshot.
            .take_while(|v| {
                // If no safepoint, this could be a abort item and is safe to install as statemap will be empty.
                let Some(safepoint) = v.safepoint else {
                    return true;
                };

                self.snapshot_version >= safepoint
            })
            // filter out the ones that can't be serialized
            .filter_map(|v| {
                // If no safepoint, this could be a abort item and is safe to install as statemap will be empty.
                let Some(safepoint) = v.safepoint else {
                    return Some(v.version);
                };

                // If there is no version matching the safepoint, then it is safe to install
                let Some(safepoint_pointing_item) = self.queue.get(&safepoint) else {
                    return Some(v.version);
                };
                if safepoint_pointing_item.state == StatemapInstallState::Installed {
                    return Some(v.version);
                };
                // error!("[items_to_install] Not picking {} as safepoint={safepoint} criteria failed against={:?}", v.version, statemap_queue.get(&safepoint));

                None
            })
            // take the remaining we can install
            .collect::<Vec<u64>>()
    }
}
