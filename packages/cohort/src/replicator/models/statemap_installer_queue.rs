use ahash::RandomState;
use indexmap::IndexMap;

use crate::replicator::core::{StatemapInstallState, StatemapInstallerHashmap};

#[derive(Debug, Default)]
pub struct StatemapInstallerQueue {
    pub queue: IndexMap<u64, StatemapInstallerHashmap, RandomState>,
    pub snapshot_version: u64,
}

impl StatemapInstallerQueue {
    pub fn update_snapshot(&mut self, snapshot_version: u64) {
        self.snapshot_version = snapshot_version;
    }

    /// Insert into queue the item to install with the version as key.
    /// The items are installed in first come basis, therefore the order of the versions are not guranteed.
    pub fn insert_queue_item(&mut self, version: &u64, installer_item: StatemapInstallerHashmap) {
        self.queue.insert(*version, installer_item);
    }

    pub fn update_queue_item_state(&mut self, version: &u64, state: StatemapInstallState) {
        if let Some(item) = self.queue.get_mut(version) {
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

#[cfg(test)]
mod tests {
    use crate::replicator::core::StatemapInstallerHashmap;

    use super::StatemapInstallerQueue;

    fn create_initial_test_installer_data(version: &u64, safepoint: Option<u64>) -> StatemapInstallerHashmap {
        StatemapInstallerHashmap {
            statemaps: vec![],
            version: *version,
            safepoint,
            state: crate::replicator::core::StatemapInstallState::Awaiting,
        }
    }

    #[test]
    fn test_installer_queue() {
        let mut installer_queue = StatemapInstallerQueue::default();

        assert_eq!(installer_queue.snapshot_version, 0);

        let version = 5;
        installer_queue.insert_queue_item(
            &version,
            StatemapInstallerHashmap {
                version,
                safepoint: None,
                state: crate::replicator::core::StatemapInstallState::Awaiting,
                statemaps: vec![],
            },
        );
        let version = 3;
        installer_queue.insert_queue_item(
            &version,
            StatemapInstallerHashmap {
                version,
                safepoint: None,
                state: crate::replicator::core::StatemapInstallState::Awaiting,
                statemaps: vec![],
            },
        );

        // Count of items inserted to queue.
        assert_eq!(installer_queue.queue.len(), 2);
        // The order is not guranteed, items are inserted as they come, and not ordered by version.
        assert_eq!(installer_queue.queue.last().unwrap().0, &3);

        // Update the snapshot version to 5.
        installer_queue.update_snapshot(5);
        assert_eq!(installer_queue.snapshot_version, 5);

        let count = installer_queue.remove_installed();
        // Nothing is removed as there are no items with state `installed`.
        assert_eq!(count, Some(0));

        //  Update the state for the version 5 as installed
        installer_queue.update_queue_item_state(&5, crate::replicator::core::StatemapInstallState::Installed);
        //  Update the state for the version 3 as installed
        installer_queue.update_queue_item_state(&3, crate::replicator::core::StatemapInstallState::Installed);

        assert_eq!(
            installer_queue.queue.first().unwrap().1.state,
            crate::replicator::core::StatemapInstallState::Installed
        );
        assert_eq!(
            installer_queue.queue.last().unwrap().1.state,
            crate::replicator::core::StatemapInstallState::Installed
        );

        let count = installer_queue.remove_installed();
        // Althought version 5 and 3 are installed and safe to remove, as the snapshot is at 5, it can remove only the
        // contigous installed items till the snapshot.
        assert_eq!(count, Some(0));

        installer_queue.update_snapshot(10);

        let count = installer_queue.remove_installed();
        // If the snapshot version is an incorrect version which is not on the queue, `None` is returned.
        assert!(count.is_none());

        installer_queue.update_snapshot(3);

        let count = installer_queue.remove_installed();
        // Although version 3 is also safe to remove, it is the snapshot version and will not be removed.
        assert_eq!(count, Some(1));
        assert_eq!(installer_queue.queue.len(), 1);
    }

    #[test]
    fn test_installer_queue_items_pick_all() {
        let mut installer_queue = StatemapInstallerQueue::default();

        assert_eq!(installer_queue.snapshot_version, 0);

        let version = 2;
        let install_item = create_initial_test_installer_data(&version, None);
        installer_queue.insert_queue_item(&version, install_item);

        let version = 3;
        let install_item = create_initial_test_installer_data(&version, None);
        installer_queue.insert_queue_item(&version, install_item);

        let version = 5;
        let install_item = create_initial_test_installer_data(&version, None);
        installer_queue.insert_queue_item(&version, install_item);

        // All items match as they are all in `Awaiting` state and safepoint is `None`
        let versions_to_install = installer_queue.get_versions_to_install();
        assert_eq!(versions_to_install.len(), 3);

        installer_queue.update_queue_item_state(&2, crate::replicator::core::StatemapInstallState::Inflight);
        installer_queue.update_queue_item_state(&3, crate::replicator::core::StatemapInstallState::Installed);
        let versions_to_install = installer_queue.get_versions_to_install();
        //  Picks only item in `Awaiting` State
        assert_eq!(versions_to_install.len(), 1);
    }

    #[test]
    fn test_installer_queue_items_snapshot_less_than_safepoint() {
        let mut installer_queue = StatemapInstallerQueue::default();

        assert_eq!(installer_queue.snapshot_version, 0);

        let version = 5;
        let install_item = create_initial_test_installer_data(&version, None);
        installer_queue.insert_queue_item(&version, install_item);

        let version = 7;
        let install_item = create_initial_test_installer_data(&version, Some(3));
        installer_queue.insert_queue_item(&version, install_item);

        let version = 9;
        let install_item = create_initial_test_installer_data(&version, Some(5));
        installer_queue.insert_queue_item(&version, install_item);

        let version = 12;
        let install_item = create_initial_test_installer_data(&version, Some(8));
        installer_queue.insert_queue_item(&version, install_item);

        installer_queue.update_snapshot(6);
        let versions_to_install = installer_queue.get_versions_to_install();
        // As version 9 has safepoint greater than the snapshot, it cannot be picked. And therefore subsequent items are not picked as well.
        assert_eq!(versions_to_install.len(), 2);
    }
    #[test]
    fn test_installer_queue_items_snapshot_version_present_in_queue() {
        let mut installer_queue = StatemapInstallerQueue::default();

        assert_eq!(installer_queue.snapshot_version, 0);

        let version = 5;
        let install_item = create_initial_test_installer_data(&version, None);
        installer_queue.insert_queue_item(&version, install_item);

        let version = 7;
        let install_item = create_initial_test_installer_data(&version, Some(3));
        installer_queue.insert_queue_item(&version, install_item);

        let version = 9;
        let install_item = create_initial_test_installer_data(&version, Some(6));
        installer_queue.insert_queue_item(&version, install_item);

        let version = 12;
        let install_item = create_initial_test_installer_data(&version, Some(9));
        installer_queue.insert_queue_item(&version, install_item);

        let version = 18;
        let install_item = create_initial_test_installer_data(&version, Some(13));
        installer_queue.insert_queue_item(&version, install_item);

        installer_queue.update_snapshot(14);
        assert_eq!(installer_queue.snapshot_version, 14);

        let versions_to_install = installer_queue.get_versions_to_install();
        // As version 9 has safepoint version which is still in queue and not installed, we will not pick that record,
        // but we can still pick version 18.
        assert_eq!(versions_to_install.len(), 4);
        // Version 12 is not picked as its safepoint is a version which is not installed.
        assert!(!versions_to_install.into_iter().any(|v| v == 12));

        //  Now version 9 is installed, therefore it is safe to pick version 12 as well.
        installer_queue.update_queue_item_state(&9, crate::replicator::core::StatemapInstallState::Installed);
        let versions_to_install = installer_queue.get_versions_to_install();
        assert_eq!(versions_to_install.len(), 4);
        // Version 9 is not picked as its installed.
        assert!(!versions_to_install.into_iter().any(|v| v == 9));
    }
}
