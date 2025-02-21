use std::{collections::HashMap, fmt::Debug};

use log::{debug, info, warn};
use serde_json::Value;
use talos_suffix::{
    core::{SuffixMeta, SuffixResult},
    get_nonempty_suffix_items, Suffix, SuffixItem, SuffixTrait,
};

use super::core::CandidateDecisionOutcome;

pub trait ReplicatorSuffixItemTrait {
    fn get_safepoint(&self) -> &Option<u64>;
    fn get_statemap(&self) -> &Option<Vec<HashMap<String, Value>>>;
    fn set_safepoint(&mut self, safepoint: Option<u64>);
    fn set_decision_outcome(&mut self, decision_outcome: Option<CandidateDecisionOutcome>);
    fn set_suffix_item_installed(&mut self);
    fn is_installed(&self) -> bool;
}

pub trait ReplicatorSuffixTrait<T: ReplicatorSuffixItemTrait>: SuffixTrait<T> {
    fn set_decision_outcome(&mut self, version: u64, decision_outcome: Option<CandidateDecisionOutcome>);
    fn set_safepoint(&mut self, version: u64, safepoint: Option<u64>);
    fn set_item_installed(&mut self, version: u64);
    fn get_last_installed(&self, to_version: Option<u64>) -> Option<&SuffixItem<T>>;
    fn update_suffix_item_decision(&mut self, version: u64, decision_ver: u64) -> SuffixResult<()>;
    /// Updates the prune index.
    ///
    /// If the prune_index was updated, returns the new prune_index else returns None.
    fn update_prune_index(&mut self, version: u64) -> Option<usize>;
    /// Returns the items from suffix
    fn get_suffix_meta(&self) -> &SuffixMeta;
    fn get_message_batch_from_version(&self, from: u64, count: Option<u64>) -> Vec<&SuffixItem<T>>;
    fn installed_all_prior_decided_items(&self, version: u64) -> bool;
    fn get_by_index(&self, index: usize) -> Option<&SuffixItem<T>>;
    fn get_index_from_head(&self, version: u64) -> Option<usize>;
    fn get_suffix_len(&self) -> usize;
}

impl<T> ReplicatorSuffixTrait<T> for Suffix<T>
where
    T: ReplicatorSuffixItemTrait + Debug + Clone,
{
    fn set_decision_outcome(&mut self, version: u64, decision_outcome: Option<CandidateDecisionOutcome>) {
        if version >= self.meta.head {
            let index = self.index_from_head(version).unwrap();
            if let Some(Some(item_to_update)) = self.messages.get_mut(index) {
                item_to_update.item.set_decision_outcome(decision_outcome);
            } else {
                warn!("Unable to update decision as message with version={version} not found");
            }
        }
    }

    fn set_safepoint(&mut self, version: u64, safepoint: Option<u64>) {
        if version >= self.meta.head {
            let index = self.index_from_head(version).unwrap();
            if let Some(Some(item_to_update)) = self.messages.get_mut(index) {
                item_to_update.item.set_safepoint(safepoint);
            } else {
                warn!("Unable to update safepoint as message with version={version} not found");
            }
        }
    }

    fn set_item_installed(&mut self, version: u64) {
        if version >= self.meta.head {
            let index = self.index_from_head(version).unwrap();
            if let Some(Some(item_to_update)) = self.messages.get_mut(index) {
                item_to_update.item.set_suffix_item_installed();
            } else {
                warn!("Unable to update is_installed flag as message with version={version} not found");
                // info!("All some items on suffix.... {:?}", self.retrieve_all_some_vec_items());
            }
        }
    }

    fn installed_all_prior_decided_items(&self, version: u64) -> bool {
        if version >= self.meta.head {
            let version_index = self.index_from_head(version).unwrap();

            let start_index = self.get_meta().prune_index.unwrap_or(0);

            return self
                .messages
                .range(start_index..version_index)
                .flatten()
                .all(|i| i.is_decided && i.item.is_installed());
        }

        false
    }

    /// Updates the prune index when it is greater than the current prune index or when the current prune index is None.
    fn update_prune_index(&mut self, version: u64) -> Option<usize> {
        if self.installed_all_prior_decided_items(version) {
            let index = self.index_from_head(version).unwrap();

            match self.get_meta().prune_index {
                Some(prune_index) => {
                    if index.gt(&prune_index) {
                        info!("[update_prune_index] Updating prune index from {:?} -> {index}", self.get_meta().prune_index);
                        self.update_prune_index(Some(index));
                        return Some(index);
                    }
                    return None;
                }
                None => {
                    info!("[update_prune_index] Prune index None. Updating to {index}");
                    self.update_prune_index(Some(index));
                    return Some(index);
                }
            }
        };
        None
    }

    fn get_message_batch_from_version(&self, from: u64, count: Option<u64>) -> Vec<&SuffixItem<T>> {
        // let mut batch = vec![];
        let batch_size = match count {
            Some(c) => c as usize,
            None => self.messages.len(),
        };

        let from_index = if from > 0 {
            if let Some(index) = self.index_from_head(from) {
                index + 1
            } else {
                0
            }
        } else {
            0
        };

        get_nonempty_suffix_items(self.messages.range(from_index..)) // take only some items in suffix
            .take_while(|m| m.is_decided) // take items till we find a not decided item.
            .filter(|m| !m.item.is_installed()) // remove already installed items.
            .take(batch_size)
            .collect::<Vec<&SuffixItem<T>>>()
    }

    fn update_suffix_item_decision(&mut self, version: u64, decision_ver: u64) -> SuffixResult<()> {
        self.update_decision_suffix_item(version, decision_ver)
    }

    fn get_suffix_meta(&self) -> &SuffixMeta {
        &self.meta
    }

    fn get_last_installed(&self, to_version: Option<u64>) -> Option<&SuffixItem<T>> {
        let version = to_version?;
        let to_index = self.index_from_head(version)?;

        // It is safe to start from prune_index as we know everything prioir to that is installed.
        let from_index = self.get_meta().prune_index.unwrap_or(0);
        //
        debug!(
            "last_installed version = {version} | index = {to_index} | suffix length = {}",
            self.messages.len()
        );
        if self.messages.is_empty() || to_index > self.messages.len() - 1 {
            return None;
        };

        self.messages
            .range(from_index..=to_index)
            .flatten()
            .take_while(|&i| i.is_decided && i.item.is_installed())
            .last()
    }

    fn get_by_index(&self, index: usize) -> Option<&SuffixItem<T>> {
        let item = self.messages.get(index);

        match item {
            Some(Some(suffix_item)) => Some(suffix_item),
            _ => {
                warn!("Item not found at index {index}");
                None
            }
        }
    }
    fn get_suffix_len(&self) -> usize {
        self.messages.len()
    }

    fn get_index_from_head(&self, version: u64) -> Option<usize> {
        self.index_from_head(version)
    }
}
