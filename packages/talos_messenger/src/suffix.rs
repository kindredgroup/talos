use log::error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{borrow::BorrowMut, fmt::Debug};
use talos_certifier::model::{CandidateMessage, Decision, DecisionMessageTrait};
use talos_suffix::{
    core::{SuffixMeta, SuffixResult},
    errors::SuffixError,
    Suffix, SuffixItem, SuffixTrait,
};

use crate::{core::MessengerCommitActions, models::commit_actions::publish::OnCommitActions, utlis::get_allowed_commit_actions};

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum SuffixItemState {
    Awaiting,
    Inflight,
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
        let CandidateMessage { version, on_commit, .. } = &candidate;

        let (state, commit_actions) = match on_commit {
            Some(actions) => {
                if let Some(on_commit_actions_parsed) = get_allowed_commit_actions(version, &actions) {
                    // Has on_commit_actions for messenger to act on.
                    // error!("Commit Actions... {on_commit_actions_parsed:?}");
                    (SuffixItemState::Awaiting, Some(on_commit_actions_parsed))
                } else {
                    // There are on_commit actions, but not the ones required by messenger
                    (SuffixItemState::Complete(SuffixItemCompleteStateReason::NoRelavantCommitActions), None)
                }
            }
            // There are no on_commit actions
            None => (SuffixItemState::Complete(SuffixItemCompleteStateReason::NoCommitActions), None),
        };
        MessengerCandidate {
            candidate,
            safepoint: None,
            decision: None,
            state,
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
    fn get_commit_actions(&self) -> Option<Box<Value>> {
        self.candidate.on_commit.clone()
        // &None
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
    fn get_commit_actions(&self) -> Option<Box<Value>>;

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

    fn get_suffix_items_to_process(&self) -> Vec<MessengerCommitActions>;
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

    fn get_suffix_items_to_process(&self) -> Vec<MessengerCommitActions> {
        // let debug_items = self
        //     .messages
        //     .iter()
        //     // Remove `None` items
        //     .flatten()
        //     // Take while contiguous decided.
        //     .take_while(|x| x.is_decided)
        //     // Filter only awaiting ones.
        //     .filter(|&x| x.item.get_state().eq(&SuffixItemState::Awaiting));

        // let count = debug_items.count();
        // if count > 0 {
        //     error!("Items decided and in awaiting count... {}", count);
        // } else {
        //     let debug_items = self
        //         .messages
        //         .iter()
        //         // Remove `None` items
        //         .flatten()
        //         .collect::<Vec<&SuffixItem<T>>>();
        //     error!("Items  \n\t\t{:#?}", debug_items);
        // }

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
                    error!("take while early exit for version {:?}", x.item_ver);
                    return false;
                };

                match self.get(*safepoint) {
                    // If we find the suffix item from the safepoint, we need to ensure that it already in `Complete` state
                    Ok(Some(safepoint_item)) => {
                        error!("State of safepoint items is {:?}", safepoint_item.item.get_state());
                        matches!(safepoint_item.item.get_state(), SuffixItemState::Complete(..))
                    }
                    // If we couldn't find the item in suffix, it could be because it was pruned and it is safe to assume that we can consider it.
                    _ => true,
                }
            })
            .filter_map(|x| {
                let Some(actions) = x.item.get_commit_actions() else {
                    return None;
                };

                Some(MessengerCommitActions {
                    version: x.item_ver,
                    commit_actions: actions.clone(),
                })
            })
            .collect();

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
