use ahash::RandomState;
use indexmap::IndexMap;

use crate::core::{StatemapInstallState, StatemapInstallerHashmap};

pub fn is_queue_item_state_match(state: StatemapInstallState) -> impl FnMut(&&StatemapInstallerHashmap) -> bool {
    move |x| x.state == state
}
pub fn is_queue_item_above_version<'a>(version: &'a u64) -> impl FnMut(&&'a StatemapInstallerHashmap) -> bool {
    move |x| {
        // If no safepoint, this could be a abort item and is safe to install as statemap will be empty.
        let Some(safepoint) = x.safepoint else {
            return true;
        };

        version >= &safepoint
    }
}
pub fn is_queue_item_serializable<'a>(queue: &'a IndexMap<u64, StatemapInstallerHashmap, RandomState>) -> impl FnMut(&&'a StatemapInstallerHashmap) -> bool {
    move |x| {
        // If no safepoint, this could be a abort item and is safe to install as statemap will be empty.
        let Some(safepoint) = x.safepoint else {
            return true;
        };

        // If there is no version matching the safepoint, then it is safe to install
        let Some(safepoint_pointing_item) = queue.get(&safepoint) else {
            return true;
        };
        safepoint_pointing_item.state == StatemapInstallState::Installed
    }
}
