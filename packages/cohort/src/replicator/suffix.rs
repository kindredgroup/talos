use std::{collections::HashMap, fmt::Debug};

use log::warn;
use serde_json::Value;
use talos_certifier::model::CandidateDecisionOutcome;
use talos_suffix::{Suffix, SuffixItem, SuffixTrait};

pub trait ReplicatorSuffixItemTrait {
    fn get_safepoint(&self) -> &Option<u64>;
    fn get_statemap(&self) -> &Option<Vec<HashMap<String, Value>>>;
    fn set_safepoint(&mut self, safepoint: Option<u64>);
    fn set_decision_outcome(&mut self, decision_outcome: Option<CandidateDecisionOutcome>);
}

pub trait ReplicatorSuffixTrait<T: ReplicatorSuffixItemTrait>: SuffixTrait<T> {
    fn set_decision(&mut self, version: u64, decision_outcome: Option<CandidateDecisionOutcome>);
    fn set_safepoint(&mut self, version: u64, safepoint: Option<u64>);
    fn get_message_batch(&self) -> Option<Vec<&SuffixItem<T>>>;
}

impl<T> ReplicatorSuffixTrait<T> for Suffix<T>
where
    T: ReplicatorSuffixItemTrait + Debug + Clone,
{
    fn set_decision(&mut self, version: u64, decision_outcome: Option<CandidateDecisionOutcome>) {
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
            let batch_messages = self.messages.range(..=prune_index).flatten().collect::<Vec<&SuffixItem<T>>>();
            return Some(batch_messages);
        }
        None
    }
}
