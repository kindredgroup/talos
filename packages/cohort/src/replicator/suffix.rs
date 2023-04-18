use std::fmt::Debug;

use log::warn;
use talos_suffix::{Suffix, SuffixItem, SuffixTrait};

use crate::replicator::core::DecisionOutcome;

use super::core::ReplicatorSuffixItemTrait;

pub trait ReplicatorSuffixTrait<T: ReplicatorSuffixItemTrait>: SuffixTrait<T> {
    fn set_decision(&mut self, version: u64, decision_outcome: Option<DecisionOutcome>);
    fn set_safepoint(&mut self, version: u64, safepoint: Option<u64>);

    fn get_message_batch(&self) -> Option<Vec<&SuffixItem<T>>>;
}

impl<T> ReplicatorSuffixTrait<T> for Suffix<T>
where
    T: ReplicatorSuffixItemTrait + Debug + Clone,
{
    fn set_decision(&mut self, version: u64, decision_outcome: Option<DecisionOutcome>) {
        if version >= self.meta.head {
            let index = self.index_from_head(version).unwrap();
            if let Some(item_to_update) = self.messages.get_mut(index).unwrap() {
                item_to_update.item.set_decision_outcome(decision_outcome);
            } else {
                warn!("Unable to update decision as message with version={version} not found");
            }
        }
    }

    fn set_safepoint(&mut self, version: u64, safepoint: Option<u64>) {
        if version >= self.meta.head {
            let index = self.index_from_head(version).unwrap();
            if let Some(item_to_update) = self.messages.get_mut(index).unwrap() {
                item_to_update.item.set_safepoint(safepoint);
            } else {
                warn!("Unable to update safepoint as message with version={version} not found");
            }
        }
    }

    fn get_message_batch(&self) -> Option<Vec<&SuffixItem<T>>> {
        if let Some(prune_index) = self.meta.prune_index {
            let batch_messages = self.messages.range(..prune_index).flatten().collect::<Vec<&SuffixItem<T>>>();
            return Some(batch_messages);
        }
        None
    }
}
