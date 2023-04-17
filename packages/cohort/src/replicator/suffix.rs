use log::warn;
use talos_suffix::{Suffix, SuffixItem};

use crate::replicator::core::{CandidateMessage, DecisionOutcome};

pub trait SuffixDecisionTrait<T> {
    fn set_decision(&mut self, version: u64, decision_outcome: Option<DecisionOutcome>);
    fn set_safepoint(&mut self, version: u64, safepoint: Option<u64>);
    fn get_message_batch(&self) -> Option<Vec<&SuffixItem<T>>>;
}

impl SuffixDecisionTrait<CandidateMessage> for Suffix<CandidateMessage> {
    fn set_decision(&mut self, version: u64, decision_outcome: Option<DecisionOutcome>) {
        if version >= self.meta.head {
            let index = self.index_from_head(version).unwrap();
            if let Some(item_to_update) = self.messages.get_mut(index).unwrap() {
                item_to_update.item.decision_outcome = decision_outcome;
            } else {
                warn!("Unable to update decision as message with version={version} not found");
            }
        }
    }

    fn set_safepoint(&mut self, version: u64, safepoint: Option<u64>) {
        if version >= self.meta.head {
            let index = self.index_from_head(version).unwrap();
            if let Some(item_to_update) = self.messages.get_mut(index).unwrap() {
                item_to_update.item.safepoint = safepoint;
            } else {
                warn!("Unable to update safepoint as message with version={version} not found");
            }
        }
    }

    fn get_message_batch(&self) -> Option<Vec<&SuffixItem<CandidateMessage>>> {
        if let Some(prune_index) = self.meta.prune_index {
            let batch_messages = self.messages.range(..prune_index).flatten().collect::<Vec<&SuffixItem<CandidateMessage>>>();
            return Some(batch_messages);
        }
        None
    }
}
