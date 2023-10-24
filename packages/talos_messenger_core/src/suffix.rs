use ahash::{HashMap, HashMapExt};
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::Debug;
use talos_certifier::model::{CandidateMessage, Decision, DecisionMessageTrait};
use talos_suffix::{core::SuffixResult, Suffix, SuffixTrait};

pub trait MessengerSuffixItemTrait: Debug + Clone {
    fn set_state(&mut self, state: SuffixItemState);
    fn set_safepoint(&mut self, safepoint: Option<u64>);
    fn set_commit_action(&mut self, commit_actions: HashMap<String, AllowedActionsMapItem>);
    fn set_decision(&mut self, decision: Decision);
    fn set_headers(&mut self, headers: HashMap<String, String>);

    fn get_state(&self) -> &SuffixItemState;
    fn get_commit_actions(&self) -> &HashMap<String, AllowedActionsMapItem>;
    fn get_action_by_key_mut(&mut self, action_key: &str) -> Option<&mut AllowedActionsMapItem>;
    fn get_safepoint(&self) -> &Option<u64>;
    fn get_headers(&self) -> &HashMap<String, String>;
    fn get_headers_mut(&mut self) -> &mut HashMap<String, String>;

    fn is_abort(&self) -> Option<bool>;
}

pub trait MessengerSuffixTrait<T: MessengerSuffixItemTrait>: SuffixTrait<T> {
    //  Setters
    /// Sets the state of an item by version.
    fn set_item_state(&mut self, version: u64, process_state: SuffixItemState);

    //  Getters

    /// Checks if suffix ready to prune
    ///
    // fn is_safe_prune() -> bool;
    /// Get the state of an item by version.
    fn get_item_state(&self, version: u64) -> Option<SuffixItemState>;
    /// Gets the suffix items eligible to process.
    fn get_suffix_items_to_process(&self) -> Vec<ActionsMapWithVersion>;
    /// Updates the decision for a version.
    fn update_item_decision<D: DecisionMessageTrait>(&mut self, version: u64, decision_version: u64, decision_message: &D, headers: HashMap<String, String>);
    /// Updates the action for a version using the action_key for lookup.
    fn increment_item_action_count(&mut self, version: u64, action_key: &str);

    /// Checks if all versions prioir to this version are already completed, and updates the prune index.
    /// If the prune index was updated, returns the new prune_index, else returns None.
    fn update_prune_index_from_version(&mut self, version: u64) -> Option<usize>;

    /// Checks if all commit actions are completed for the version
    fn are_all_actions_complete_for_version(&self, version: u64) -> SuffixResult<bool>;
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum SuffixItemState {
    AwaitingDecision,
    ReadyToProcess,
    Processing,
    PartiallyComplete,
    Complete(SuffixItemCompleteStateReason),
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum SuffixItemCompleteStateReason {
    /// When the decision is an abort
    Aborted,
    /// When there are no commit actions.
    NoCommitActions,
    /// When there are commit actions, but they are not required to be handled in messenger
    NoRelavantCommitActions,
    /// When all commit action has are completed.
    Processed,
    /// Error in processing
    ErrorProcessing,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct AllowedActionsMapItem {
    payload: Value,
    count: u32,
    total_count: u32,
}

impl AllowedActionsMapItem {
    pub fn new(payload: Value, total_count: u32) -> Self {
        AllowedActionsMapItem {
            payload,
            count: 0,
            total_count,
        }
    }

    pub fn increment_count(&mut self) {
        self.count += 1;
    }

    pub fn get_payload(&self) -> &Value {
        &self.payload
    }

    pub fn get_count(&self) -> u32 {
        self.count
    }

    pub fn is_completed(&self) -> bool {
        self.total_count > 0 && self.total_count == self.count
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct ActionsMapWithVersion {
    pub actions: HashMap<String, AllowedActionsMapItem>,
    pub version: u64,
    pub headers: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct MessengerCandidate {
    pub candidate: CandidateMessage,
    /// Safepoint received for committed outcomes from certifier.
    safepoint: Option<u64>,
    /// Decision received from certifier.
    decision: Option<Decision>,
    /// Suffix item state.
    state: SuffixItemState,
    /// Filtered actions that need to be processed by the messenger
    allowed_actions_map: HashMap<String, AllowedActionsMapItem>,
    /// Any headers from decision to be used in on-commit actions
    headers: HashMap<String, String>,
}

impl From<CandidateMessage> for MessengerCandidate {
    fn from(candidate: CandidateMessage) -> Self {
        MessengerCandidate {
            candidate,
            safepoint: None,
            decision: None,

            state: SuffixItemState::AwaitingDecision,
            allowed_actions_map: HashMap::new(),
            headers: HashMap::new(),
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

    fn set_commit_action(&mut self, commit_actions: HashMap<String, AllowedActionsMapItem>) {
        self.allowed_actions_map = commit_actions
    }

    fn get_state(&self) -> &SuffixItemState {
        &self.state
    }

    fn get_safepoint(&self) -> &Option<u64> {
        &self.safepoint
    }
    fn get_commit_actions(&self) -> &HashMap<String, AllowedActionsMapItem> {
        &self.allowed_actions_map
        // &None
    }

    fn is_abort(&self) -> Option<bool> {
        Some(self.decision.clone()?.eq(&Decision::Aborted))
    }

    fn get_action_by_key_mut(&mut self, action_key: &str) -> Option<&mut AllowedActionsMapItem> {
        self.allowed_actions_map.get_mut(action_key)
    }

    fn set_headers(&mut self, headers: HashMap<String, String>) {
        self.headers.extend(headers);
    }

    fn get_headers(&self) -> &HashMap<String, String> {
        &self.headers
    }

    fn get_headers_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.headers
    }
}

impl<T> MessengerSuffixTrait<T> for Suffix<T>
where
    T: MessengerSuffixItemTrait,
{
    fn set_item_state(&mut self, version: u64, process_state: SuffixItemState) {
        if let Some(item_to_update) = self.get_mut(version) {
            item_to_update.item.set_state(process_state)
        }
    }

    fn get_item_state(&self, version: u64) -> Option<SuffixItemState> {
        if let Ok(Some(suffix_item)) = self.get(version) {
            Some(suffix_item.item.get_state().clone())
        } else {
            None
        }
    }

    fn get_suffix_items_to_process(&self) -> Vec<ActionsMapWithVersion> {
        let items = self
            .messages
            .iter()
            // Remove `None` items
            .flatten()
            // Filter only the items awaiting to be processed.
            .filter(|&x| x.item.get_state().eq(&SuffixItemState::ReadyToProcess))
            // Take while contiguous ones, whose safepoint is already processed.
            .take_while(|&x| {
                let Some(safepoint) = x.item.get_safepoint() else {
                    return false;
                };

                match self.get(*safepoint) {
                    // If we find the suffix item from the safepoint, we need to ensure that it already in `Complete` state
                    Ok(Some(safepoint_item)) => {
                        matches!(safepoint_item.item.get_state(), SuffixItemState::Complete(..))
                    }
                    // If we couldn't find the item in suffix, it could be because it was pruned and it is safe to assume that we can consider it.
                    _ => true,
                }
            })
            .map(|x| ActionsMapWithVersion {
                version: x.item_ver,
                actions: x.item.get_commit_actions().clone(),
                headers: x.item.get_headers().clone(),
            })
            .collect();

        items
    }

    fn update_item_decision<D: DecisionMessageTrait>(&mut self, version: u64, decision_version: u64, decision_message: &D, headers: HashMap<String, String>) {
        let _ = self.update_decision_suffix_item(version, decision_version);

        if let Some(item_to_update) = self.get_mut(version) {
            // If abort, mark the item as processed.
            if decision_message.is_abort() {
                item_to_update
                    .item
                    .set_state(SuffixItemState::Complete(crate::suffix::SuffixItemCompleteStateReason::Aborted));
            } else if item_to_update.item.get_state().eq(&SuffixItemState::AwaitingDecision) {
                item_to_update.item.set_state(SuffixItemState::ReadyToProcess);
            }

            item_to_update.item.set_decision(decision_message.get_decision().clone());
            item_to_update.item.set_safepoint(decision_message.get_safepoint());
            item_to_update.item.set_headers(headers);
        }
    }

    fn increment_item_action_count(&mut self, version: u64, action_key: &str) {
        if let Some(item_to_update) = self.get_mut(version) {
            if let Some(action) = item_to_update.item.get_action_by_key_mut(action_key) {
                action.increment_count();
            } else {
                warn!("Could not update the action as item with version={version} does not have action_key={action_key}! ");
            }
        } else {
            warn!("Could not update the action as item with version={version} was not found! ");
        }
    }

    fn update_prune_index_from_version(&mut self, version: u64) -> Option<usize> {
        let current_prune_index = self.get_meta().prune_index;

        let start_index = current_prune_index.unwrap_or(0);

        let end_index = match self.index_from_head(version) {
            Some(index) if index > start_index => index,
            _ => self.suffix_length() - 1,
        };

        debug!(
            "[Update prune index] Calculating prune index in suffix slice between index {start_index} <-> {end_index}. Current prune index version {current_prune_index:?}.",
        );

        // 1. Get the last contiguous item that is completed.
        let safe_prune_version = self
            .messages
            .range(start_index..=end_index)
            .flatten()
            .take_while(|item| matches!(item.item.get_state(), SuffixItemState::Complete(..)))
            .last()?
            .item_ver;

        // 2. Update the prune index.
        let index = self.index_from_head(safe_prune_version)?;

        self.update_prune_index(index.into());
        debug!("[Update prune index] Prune version updated to {index} (version={safe_prune_version}");

        Some(index)
    }

    fn are_all_actions_complete_for_version(&self, version: u64) -> SuffixResult<bool> {
        if let Ok(Some(item)) = self.get(version) {
            Ok(item.item.get_commit_actions().iter().all(|(_, x)| x.is_completed()))
        } else {
            warn!("could not find item for version={version}");
            Err(talos_suffix::errors::SuffixError::ItemNotFound(version, None))
        }
    }
}
