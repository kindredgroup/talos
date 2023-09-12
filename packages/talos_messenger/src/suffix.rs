use log::error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{borrow::BorrowMut, fmt::Debug};
use talos_certifier::model::{CandidateMessage, Decision, DecisionMessageTrait};
use talos_suffix::{
    core::{SuffixMeta, SuffixResult},
    Suffix, SuffixItem, SuffixTrait,
};

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum SuffixItemState {
    Awaiting,
    Inflight(u32),
    Complete(SuffixItemCompleteStateReason),
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum SuffixItemCompleteStateReason {
    Aborted,
    NoCommitActions,
    NoRelavantCommitActions,
    Processed,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct MessengerCandidate {
    pub candidate: CandidateMessage,

    pub safepoint: Option<u64>,

    pub decision: Option<Decision>,

    pub state: SuffixItemState,
}

impl From<CandidateMessage> for MessengerCandidate {
    fn from(candidate: CandidateMessage) -> Self {
        MessengerCandidate {
            candidate,
            safepoint: None,
            decision: None,
            state: SuffixItemState::Awaiting,
        }
    }
}

impl MessengerSuffixItemTrait for MessengerCandidate {
    fn set_safepoint(&mut self, safepoint: Option<u64>) {
        self.safepoint = safepoint;
    }

    fn set_decision(&mut self, decision: Decision) {
        self.decision = Some(decision);
    }

    fn set_state(&mut self, state: SuffixItemState) {
        self.state = state;
    }

    fn get_state(&self) -> &SuffixItemState {
        &self.state
    }

    fn get_safepoint(&self) -> &Option<u64> {
        &self.safepoint
    }
    fn get_commit_actions(&self) -> &Option<Value> {
        &self.candidate.on_commit
    }

    fn is_abort(&self) -> Option<bool> {
        Some(self.decision.clone()?.eq(&Decision::Aborted))
    }
}

pub trait MessengerSuffixItemTrait {
    //  Setters
    fn set_safepoint(&mut self, safepoint: Option<u64>);
    fn set_decision(&mut self, decision: Decision);
    fn set_state(&mut self, state: SuffixItemState);
    //  Getters
    fn get_state(&self) -> &SuffixItemState;
    fn get_safepoint(&self) -> &Option<u64>;
    fn get_commit_actions(&self) -> &Option<Value>;

    fn is_abort(&self) -> Option<bool>;
}

pub trait MessengerSuffixTrait<T: MessengerSuffixItemTrait>: SuffixTrait<T> {
    //  Setters
    fn set_item_state(&mut self, version: u64, process_state: SuffixItemState);

    //  Getters
    fn get_mut(&mut self, version: u64) -> Option<&mut SuffixItem<T>>;
    fn get_last_installed(&self, to_version: Option<u64>) -> Option<&SuffixItem<T>>;
    // fn update_suffix_item_decision(&mut self, version: u64, decision_ver: u64) -> SuffixResult<()>;
    fn get_suffix_meta(&self) -> &SuffixMeta;
    fn get_message_batch_from_version(&self, from: u64, count: Option<u64>) -> Option<Vec<&SuffixItem<T>>>;
    fn installed_all_prior_decided_items(&self, version: u64) -> bool;

    fn get_on_commit_actions_to_process(&self) -> Vec<&SuffixItem<T>>;
    // updates
    fn update_prune_index(&mut self, version: u64);
    fn update_item_decision<D: DecisionMessageTrait>(&mut self, version: u64, decision_message: &D);
}

impl<T> MessengerSuffixTrait<T> for Suffix<T>
where
    T: MessengerSuffixItemTrait + Debug + Clone,
{
    // TODO: GK - Elevate this to core suffix
    fn get_mut(&mut self, version: u64) -> Option<&mut SuffixItem<T>> {
        let index = self.index_from_head(version)?;
        self.messages.get_mut(index)?.as_mut()
    }

    fn set_item_state(&mut self, version: u64, process_state: SuffixItemState) {
        if let Some(item_to_update) = self.get_mut(version) {
            item_to_update.item.set_state(process_state)
        }
    }

    fn get_last_installed(&self, to_version: Option<u64>) -> Option<&SuffixItem<T>> {
        todo!()
    }

    // TODO: GK - Elevate this to core suffix
    fn get_suffix_meta(&self) -> &SuffixMeta {
        &self.meta
    }

    fn get_message_batch_from_version(&self, from: u64, count: Option<u64>) -> Option<Vec<&SuffixItem<T>>> {
        todo!()
    }

    fn installed_all_prior_decided_items(&self, version: u64) -> bool {
        todo!()
    }

    fn get_on_commit_actions_to_process(&self) -> Vec<&SuffixItem<T>> {
        let debug_items = self
            .messages
            .iter()
            // Remove `None` items
            .flatten()
            // Take while contiguous decided.
            .take_while(|x| x.is_decided)
            // Filter only awaiting ones.
            .filter(|&x| x.item.get_state().eq(&SuffixItemState::Awaiting));

        error!("Items decided and in awaiting count... {}", debug_items.count());

        let items = self
            .messages
            .iter()
            // Remove `None` items
            .flatten()
            // Take while contiguous decided.
            .take_while(|x| x.is_decided)
            // Filter only awaiting ones.
            .filter(|&x| x.item.get_state().eq(&SuffixItemState::Awaiting))
            // Filter out aborted items.
            // Ideally this wouldn't happen as before this function is called we mark
            //  candidate messages with no commit actions as `MessengerSuffixItemState::Processed` as soon as we receive them.
            //  decision messages with no aborts as `MessengerSuffixItemState::Processed` as soon as we receive them.
            .filter(|x| !x.item.is_abort().unwrap())
            // Filter out candidate messages without on_commit actions.
            // Ideally this wouldn't happen as before this function was called we marked
            //  candidate messages with no commit actions as `MessengerSuffixItemState::Processed` as soon as we receive them.
            .filter(|x| x.item.get_commit_actions().is_some())
            // Take while contiguous ones, whose safepoint is already processed.
            .take_while(|&x| {
                let Some(safepoint) = x.item.get_safepoint() else {
                    return false;
                };

                match self.get(*safepoint) {
                    // If we find the suffix item from the safepoint, we need to ensure that it is processed
                    Ok(Some(safepoint_item)) => matches!(safepoint_item.item.get_state(), SuffixItemState::Complete(..)),
                    // safepoint_item.item.get_state().eq(&MessengerSuffixItemState::Complete(..)),
                    // If we couldn't find the item in suffix, it could be because it was pruned and it is safe to assume that we can consider it.
                    Ok(None) => true,
                    _ => false,
                }
            })
            .collect::<Vec<&SuffixItem<T>>>();

        items
    }

    fn update_prune_index(&mut self, version: u64) {
        todo!()
    }

    fn update_item_decision<D: DecisionMessageTrait>(&mut self, version: u64, decision_message: &D) {
        if let Some(item_to_update) = self.get_mut(version) {
            item_to_update.item.set_decision(decision_message.get_decision().clone());
            item_to_update.item.set_safepoint(decision_message.get_safepoint());
        }
    }
}
