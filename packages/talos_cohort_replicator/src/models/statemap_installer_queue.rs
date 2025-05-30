use std::ops::ControlFlow;

use ahash::RandomState;
use indexmap::IndexMap;
use tracing::{debug, info};

use crate::{
    core::{StatemapInstallState, StatemapInstallerHashmap},
    utils::installer_utils::{is_queue_item_above_version, is_queue_item_serializable, is_queue_item_state_match},
};

#[derive(Debug, Default)]
pub struct DbgQueueFilterSummary<T> {
    pub filter_enter_count: usize,
    pub filter_exit_count: usize,
    pub filter_reject_items: Vec<T>,
}
#[derive(Debug, Default)]
pub struct DbgQueueInstallItemsSummary<T> {
    pub installable_items: Vec<T>,
    pub filter_steps_insights: Vec<DbgQueueFilterSummary<T>>,
}

#[derive(Debug, Default)]
pub struct StatemapInstallerQueue {
    pub queue: IndexMap<u64, StatemapInstallerHashmap, RandomState>,
    pub snapshot_version: u64,
}

impl StatemapInstallerQueue {
    pub fn update_snapshot(&mut self, snapshot_version: u64) {
        if snapshot_version > self.snapshot_version {
            debug!("Updating snapshot_version to {}", snapshot_version);
            self.snapshot_version = snapshot_version;
        }
    }

    /// Insert into queue the item to install with the version as key.
    /// The items are installed in first come basis, therefore the order of the versions are not guranteed.
    pub fn insert_queue_item(&mut self, version: &u64, installer_item: StatemapInstallerHashmap) {
        self.queue.insert(*version, installer_item);
    }

    /**
     * Returns items' enqueueing time
     */
    pub fn update_queue_item_state(&mut self, version: &u64, state: StatemapInstallState) -> Option<i128> {
        let item = self.queue.get_mut(version)?;
        item.state = state;
        Some(item.timestamp)
    }

    pub fn prune_till_version(&mut self, version: u64) -> Option<u64> {
        let index = self.queue.get_index_of(&version)?;

        Some(self.prune_till_index(index))
    }

    pub fn prune_till_index(&mut self, index: usize) -> u64 {
        let queue_length = self.queue.len();

        if index < queue_length {
            let items = self.queue.drain(..=index);
            let count = items.len() as u64;
            drop(items);
            if count > 0 {
                info!(
                    "Pruned {count} items in statemap queue. Length changed from {queue_length} to {} | snapshot_version = {} ",
                    self.queue.len(),
                    self.snapshot_version
                );
            }

            count
        } else {
            0
        }
    }

    /// Filter items in queue based on the `StatemapInstallState`
    pub fn filter_items_by_state(&self, state: StatemapInstallState) -> impl Iterator<Item = &StatemapInstallerHashmap> {
        self.queue.values().filter(is_queue_item_state_match(state))
    }

    #[deprecated]
    #[allow(dead_code)]
    //TODO: GK - keeping it for now, although it is not used anymore. Will remove it in future if it is not needed back.
    pub(crate) fn dbg_get_versions_to_install(&self) -> DbgQueueInstallItemsSummary<&StatemapInstallerHashmap> {
        let mut intermediate_steps = vec![];

        let items_awaiting: Vec<&StatemapInstallerHashmap> = self.queue.values().filter(is_queue_item_state_match(StatemapInstallState::Awaiting)).collect();

        // Capture for debug the items entering and exiting
        let filter_on_awaiting_criteria = DbgQueueFilterSummary::<&StatemapInstallerHashmap> {
            filter_enter_count: self.queue.len(),
            filter_exit_count: items_awaiting.len(),
            filter_reject_items: vec![],
        };

        let vec1 = vec![];
        let vec2 = vec![];

        let mut closure_above_version = is_queue_item_above_version(&self.snapshot_version);
        let x: ControlFlow<_, _> = items_awaiting.iter().try_fold((vec1, vec2), |mut acc, x| {
            if closure_above_version(x) {
                acc.0.push(*x);
                ControlFlow::Continue(acc)
            } else {
                acc.1.push(*x);
                ControlFlow::Break(acc)
            }
        });

        let (items_safepoint_match, items_safepoint_fail) = match x {
            ControlFlow::Continue(v) => v,
            ControlFlow::Break(v) => v,
        };

        let filter_on_snapshot_criteria = DbgQueueFilterSummary::<&StatemapInstallerHashmap> {
            filter_enter_count: items_awaiting.len(),
            filter_exit_count: items_safepoint_match.len(),
            filter_reject_items: items_safepoint_fail,
        };

        let (final_items, items_non_serializable): (Vec<&StatemapInstallerHashmap>, Vec<&StatemapInstallerHashmap>) =
            items_safepoint_match.into_iter().partition(is_queue_item_serializable(&self.queue));

        let filter_on_serialization_criteria = DbgQueueFilterSummary::<&StatemapInstallerHashmap> {
            filter_enter_count: filter_on_snapshot_criteria.filter_exit_count,
            filter_exit_count: final_items.len(),
            filter_reject_items: items_non_serializable,
        };

        intermediate_steps.push(filter_on_awaiting_criteria);
        intermediate_steps.push(filter_on_snapshot_criteria);
        intermediate_steps.push(filter_on_serialization_criteria);

        DbgQueueInstallItemsSummary {
            installable_items: final_items,
            filter_steps_insights: intermediate_steps,
        }
    }

    pub fn get_versions_to_install(&self) -> Vec<u64> {
        self.queue
            .values()
            // filter items in `Awaiting` state
            .filter(is_queue_item_state_match(StatemapInstallState::Awaiting))
            // Get items whose safepoint is below the snapshot.
            .take_while(is_queue_item_above_version(&self.snapshot_version))
            // filter items safe to be serialized
            .filter(is_queue_item_serializable(&self.queue))
            // map the version
            .map(|x| x.version)
            // collect the iterator of versions into a vec
            .collect::<Vec<u64>>()
    }

    pub fn get_last_contiguous_installed_version(&self) -> Option<u64> {
        // If queue is empty, there are not installed items.
        if self.queue.is_empty() {
            return None;
        }

        // If queue is not empty and the snapshot version is below this version, then we find the last_contiguous installed version.
        let start = if let Some(version_index) = self.queue.get_index_of(&self.snapshot_version) {
            version_index
        } else {
            0
        };

        let (last_installed_version, _) = self
            .queue
            .get_range(start..)?
            .iter()
            .take_while(|(_, statemap_installer_item)| statemap_installer_item.state == StatemapInstallState::Installed)
            .last()?;

        Some(*last_installed_version)
    }
}

#[cfg(test)]
mod tests {
    use crate::core::StatemapInstallerHashmap;

    use super::StatemapInstallerQueue;

    fn create_initial_test_installer_data(version: &u64, safepoint: Option<u64>) -> StatemapInstallerHashmap {
        StatemapInstallerHashmap {
            timestamp: 0,
            statemaps: vec![],
            version: *version,
            safepoint,
            state: crate::core::StatemapInstallState::Awaiting,
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
                timestamp: 0,
                version,
                safepoint: None,
                state: crate::core::StatemapInstallState::Awaiting,
                statemaps: vec![],
            },
        );
        let version = 3;
        installer_queue.insert_queue_item(
            &version,
            StatemapInstallerHashmap {
                timestamp: 0,
                version,
                safepoint: None,
                state: crate::core::StatemapInstallState::Awaiting,
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

        let count = installer_queue.prune_till_version(installer_queue.snapshot_version);
        // If snapshot is updated, we prune everything below it
        assert_eq!(count, Some(1));

        //  Update the state for the version 5 as installed
        let upd_5 = installer_queue.update_queue_item_state(&5, crate::core::StatemapInstallState::Installed);
        //  Update the state for the version 3 as installed
        let upd_3 = installer_queue.update_queue_item_state(&3, crate::core::StatemapInstallState::Installed);

        assert!(upd_5.is_none());
        // As the items are put not in order, 3 is not removed.
        assert!(upd_3.is_some());
        assert_eq!(installer_queue.queue.len(), 1);
        // assert_eq!(installer_queue.queue.get(&5).unwrap().1.state, crate::core::StatemapInstallState::Installed);

        // let count = installer_queue.prune_till_version(installer_queue.snapshot_version);
        // // Althought version 5 and 3 are installed and safe to remove, as the snapshot is at 5, it can remove only the
        // // contigous installed items till the snapshot.
        // assert_eq!(count, 0);

        // installer_queue.update_snapshot(10);

        // let count = installer_queue.prune_till_version(installer_queue.snapshot_version);
        // // If the snapshot version is an incorrect version which is not on the queue, `None` is returned.
        // assert_eq!(count, 0);

        // installer_queue.update_snapshot(3);

        // let count = installer_queue.prune_till_version(installer_queue.snapshot_version);
        // // Although version 3 is also safe to remove, it is the snapshot version and will not be removed.
        // assert_eq!(count, 1);
        // assert_eq!(installer_queue.queue.len(), 1);
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

        installer_queue.update_queue_item_state(&2, crate::core::StatemapInstallState::Inflight);
        installer_queue.update_queue_item_state(&3, crate::core::StatemapInstallState::Installed);
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
        installer_queue.update_queue_item_state(&9, crate::core::StatemapInstallState::Installed);
        let versions_to_install = installer_queue.get_versions_to_install();
        assert_eq!(versions_to_install.len(), 4);
        // Version 9 is not picked as its installed.
        assert!(!versions_to_install.into_iter().any(|v| v == 9));
    }
}
