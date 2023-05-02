use std::{collections::HashMap, fmt::Debug, ops::ControlFlow};

use log::warn;
use serde_json::Value;
use talos_suffix::{core::SuffixResult, get_nonempty_suffix_items, Suffix, SuffixItem, SuffixTrait};

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
    fn update_suffix_item_decision(&mut self, version: u64, decision_ver: u64) -> SuffixResult<()>;
    fn update_prune_index(&mut self, version: u64);
    /// Returns the items from suffix
    fn get_message_batch(&self, count: Option<u64>) -> Option<Vec<&SuffixItem<T>>>;
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
            }
        }
    }

    fn update_prune_index(&mut self, version: u64) {
        if self.are_prior_items_decided(version) {
            let index = self.index_from_head(version).unwrap();
            self.update_prune_index(Some(index));
        }
    }

    fn get_message_batch(&self, count: Option<u64>) -> Option<Vec<&SuffixItem<T>>> {
        let mut batch = vec![];
        let batch_size = match count {
            Some(c) => c as usize,
            None => self.messages.len(),
        };

        get_nonempty_suffix_items(self.messages.iter()) // take only some items in suffix
            .take_while(|m| m.is_decided) // narrow down to only the ones those are decided.
            .try_for_each(|m| {
                if !m.item.is_installed() {
                    batch.push(m);

                    if batch.len() == batch_size {
                        return ControlFlow::Break(());
                    }
                }
                ControlFlow::Continue(())
            });

        if batch.is_empty() {
            None
        } else {
            Some(batch)
        }
    }

    fn update_suffix_item_decision(&mut self, version: u64, decision_ver: u64) -> SuffixResult<()> {
        self.update_decision_suffix_item(version, decision_ver)
    }
}
