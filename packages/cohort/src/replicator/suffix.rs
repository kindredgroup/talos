use std::{collections::HashMap, fmt::Debug};

use log::warn;
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
    fn update_prune_index(&mut self, version: u64);
    /// Returns the items from suffix
    fn get_suffix_meta(&self) -> &SuffixMeta;
    fn get_message_batch_from_version(&self, from: u64, count: Option<u64>) -> Option<Vec<&SuffixItem<T>>>;
    fn installed_all_prior_decided_items(&self, version: u64) -> bool;
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
            let index = self.index_from_head(version).unwrap();

            return self.messages.range(..index).flatten().all(|i| i.is_decided && i.item.is_installed());
        }

        false
    }

    fn update_prune_index(&mut self, version: u64) {
        if self.installed_all_prior_decided_items(version) {
            let index = self.index_from_head(version).unwrap();
            self.update_prune_index(Some(index));
        }
    }

    fn get_message_batch_from_version(&self, from: u64, count: Option<u64>) -> Option<Vec<&SuffixItem<T>>> {
        // let mut batch = vec![];
        let batch_size = match count {
            Some(c) => c as usize,
            None => self.messages.len(),
        };

        let from_index = if from != 0 { self.index_from_head(from).unwrap() + 1 } else { 0 };

        let items = get_nonempty_suffix_items(self.messages.range(from_index..)) // take only some items in suffix
            .take_while(|m| m.is_decided) // take items till we find a not decided item.
            .filter(|m| !m.item.is_installed()) // remove already installed items.
            .take(batch_size)
            .collect::<Vec<&SuffixItem<T>>>();
        // let items_picked_in_suffix_batch = items.iter().map(|&item| item.item_ver).collect::<Vec<u64>>();

        // error!("Items picked in this batch from_version={from} as index={from_index} \n versions={items_picked_in_suffix_batch:?}");
        if !items.is_empty() {
            Some(items)
        } else {
            None
        }
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
        self.messages.range(..to_index).flatten().rev().find(|&i| i.is_decided && i.item.is_installed())
    }
}
