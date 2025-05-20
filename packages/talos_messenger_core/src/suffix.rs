use ahash::{HashMap, HashMapExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::Debug;
use strum::{Display, EnumString};
use talos_certifier::model::{Decision, DecisionMessageTrait};
use talos_suffix::{core::SuffixResult, Suffix, SuffixTrait};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tracing::{debug, warn};

use crate::models::MessengerCandidateMessage;

pub trait MessengerSuffixItemTrait: Debug + Clone {
    fn set_state(&mut self, state: SuffixItemState);
    fn set_safepoint(&mut self, safepoint: Option<u64>);
    fn set_commit_action(&mut self, commit_actions: HashMap<String, AllowedActionsMapItem>);
    fn set_decision(&mut self, decision: Decision);
    fn set_headers(&mut self, headers: HashMap<String, String>);

    fn get_state(&self) -> &SuffixItemState;
    fn get_state_transition_timestamps(&self) -> &HashMap<MessengerStateTransitionTimestamps, TimeStamp>;
    fn get_commit_actions(&self) -> &HashMap<String, AllowedActionsMapItem>;
    fn get_action_by_key_mut(&mut self, action_key: &str) -> Option<&mut AllowedActionsMapItem>;
    fn get_safepoint(&self) -> &Option<u64>;
    fn get_headers(&self) -> &HashMap<String, String>;
    fn get_headers_mut(&mut self) -> &mut HashMap<String, String>;

    fn is_abort(&self) -> Option<bool>;
}

#[cfg(test)]
pub trait MessengerSuffixAssertionTrait<T: MessengerSuffixItemTrait>: SuffixTrait<T> {
    /// Asserts suffix `head` and `prune_index`.
    ///
    /// It is important to understand where and why the `head` and `prune_index` are at any time.
    ///
    /// - `head` gets updated either at the begining when first none-zero version is inserted into suffix.
    /// - `prune_index` gets updated when contiguous items in suffix from the head are in the final state `SuffixItemState::Complete(..)`.
    fn assert_suffix_head_and_prune_index(&self, expected_head: u64, expected_prune_index: Option<usize>) {
        let suffix_meta = self.get_meta();
        assert_eq!(
            suffix_meta.head, expected_head,
            "Expected head to be {expected_head} but got {:?}",
            suffix_meta.head
        );
        assert_eq!(
            suffix_meta.prune_index, expected_prune_index,
            "Expected prune_index to be {expected_prune_index:?} but got {:?}",
            suffix_meta.prune_index
        );
    }

    /// Asserts the state of an item on suffix.
    /// Checks for `SuffixItemState` actual vs expected for a version.
    fn assert_item_state(&self, version: u64, expected_state: &SuffixItemState) {
        let suffix_item = self
            .get(version)
            .unwrap()
            .unwrap_or_else(|| panic!("Expected version {version} to be in suffix"));
        let actual_state = suffix_item.item.get_state();

        assert_eq!(
            actual_state, expected_state,
            "Expected state for version {version} is {expected_state:?} but got {actual_state:?}"
        );
    }
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
    fn update_prune_index_from_version(&mut self, version: u64) -> Option<(usize, u64)>;

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

type TimeStamp = String;

/// Internal timings from messenger for a candidate received.
/// These timings help in debugging the time taken between various state changes.
#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, EnumString, Display, Hash)]
pub enum MessengerStateTransitionTimestamps {
    /// Set when SuffixItemState::AwaitingDecision
    #[strum(serialize = "messengerCandidateReceivedTimestamp")]
    CandidateReceived,
    /// Set when SuffixItemState::ReadyToProcess
    #[strum(serialize = "messengerDecisionReceivedTimestamp")]
    DecisionReceived,
    /// Set when SuffixItemState::Processing
    #[strum(serialize = "messengerStartOnCommitActionsTimestamp")]
    StartOnCommitActions,
    // /// Not required for timings
    #[strum(disabled)]
    InProgressOnCommitActions,
    /// Set when SuffixItemState::Complete
    /// Irrespective of the reason for completion, we just capture the timing based on the final state.
    #[strum(serialize = "messengerEndOnCommitActionsTimestamp")]
    EndOnCommitActions,
}

impl From<SuffixItemState> for MessengerStateTransitionTimestamps {
    fn from(value: SuffixItemState) -> Self {
        match value {
            SuffixItemState::AwaitingDecision => MessengerStateTransitionTimestamps::CandidateReceived,
            SuffixItemState::ReadyToProcess => MessengerStateTransitionTimestamps::DecisionReceived,
            SuffixItemState::Processing => MessengerStateTransitionTimestamps::StartOnCommitActions,
            SuffixItemState::PartiallyComplete => MessengerStateTransitionTimestamps::InProgressOnCommitActions,
            SuffixItemState::Complete(_) => MessengerStateTransitionTimestamps::EndOnCommitActions,
        }
    }
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
    pub candidate: MessengerCandidateMessage,
    /// Safepoint received for committed outcomes from certifier.
    safepoint: Option<u64>,
    /// Decision received from certifier.
    decision: Option<Decision>,
    /// Suffix item state.
    state: SuffixItemState,
    /// Suffix state transition timestamps.
    state_transition_ts: HashMap<MessengerStateTransitionTimestamps, TimeStamp>,
    /// Filtered actions that need to be processed by the messenger
    allowed_actions_map: HashMap<String, AllowedActionsMapItem>,
    /// Any headers from decision to be used in on-commit actions
    headers: HashMap<String, String>,
}

impl From<MessengerCandidateMessage> for MessengerCandidate {
    fn from(candidate: MessengerCandidateMessage) -> Self {
        let state = SuffixItemState::AwaitingDecision;
        let mut state_transition_ts: HashMap<MessengerStateTransitionTimestamps, TimeStamp> = HashMap::new();
        let timestamp = OffsetDateTime::now_utc().format(&Rfc3339).ok().unwrap();
        state_transition_ts.insert(state.clone().into(), timestamp);

        MessengerCandidate {
            candidate,
            safepoint: None,
            decision: None,
            state,
            state_transition_ts,
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
        self.state = state.clone();

        let timestamp = OffsetDateTime::now_utc().format(&Rfc3339).ok().unwrap();
        self.state_transition_ts.insert(state.into(), timestamp);
    }

    fn set_commit_action(&mut self, commit_actions: HashMap<String, AllowedActionsMapItem>) {
        // serde_json::Value fields hold a lot of memory. Therefore the duplications of `on_commit` and `allowed_actions_map` causes lot of memory overhead.
        // `on_commit` is not longer required once we move the actions to `allowed_actions_map`, and therefore it is fine to set it to `None`.

        // TODO: GK - Address the memory overhead due to the use of `serde_json::Value` in a better way. Explore using `serde_json::RawValue` to just retain the raw value
        // and parse it only when really required.
        self.candidate.on_commit = None;
        self.allowed_actions_map = commit_actions;
    }

    fn get_state(&self) -> &SuffixItemState {
        &self.state
    }
    fn get_state_transition_timestamps(&self) -> &HashMap<MessengerStateTransitionTimestamps, TimeStamp> {
        &self.state_transition_ts
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

#[cfg(test)]
impl<T> MessengerSuffixAssertionTrait<T> for Suffix<T> where T: MessengerSuffixItemTrait {}

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
        let current_prune_index = self.get_meta().prune_index;

        let start_index = current_prune_index.unwrap_or(0);

        // let start_ms = Instant::now();
        let items: Vec<ActionsMapWithVersion> = self
            .messages
            // we know from start_index = prune_index, everything prioir to this is already completed.
            // This helps in taking a smaller slice out of the suffix to iterate over.
            .range(start_index..)
            // Remove `None` items
            .flatten()
            // Filter only the items awaiting to be processed.
            .filter(|&x| x.item.get_state().eq(&SuffixItemState::ReadyToProcess))
            // Take while contiguous ones, whose safepoint is already processed.
            .filter(|&x| {
                let Some(safepoint) = x.item.get_safepoint() else {
                    return false;
                };

                match self.get(*safepoint) {
                    // If we find the suffix item from the safepoint, we need to ensure that it already in `Complete`, `PartiallyComplete` or `Processing` state
                    Ok(Some(safepoint_item)) => {
                        matches!(
                            safepoint_item.item.get_state(),
                            SuffixItemState::Processing | SuffixItemState::PartiallyComplete | SuffixItemState::Complete(..)
                        )
                    }
                    // If we couldn't find the item in suffix, it could be because it was pruned and it is safe to assume that we can consider it.
                    _ => true,
                }
            })
            // add timings related headers.
            .map(|x| {
                let mut headers = x.item.get_headers().clone();
                // Add the state timestamps
                let state_timestamps_headers = x
                    .item
                    .get_state_transition_timestamps()
                    // .clone()
                    .iter()
                    .map(|(state, ts)| (state.to_string(), ts.clone()))
                    .collect::<HashMap<String, String>>();

                headers.extend(state_timestamps_headers);

                ActionsMapWithVersion {
                    version: x.item_ver,
                    actions: x.item.get_commit_actions().clone(),
                    headers,
                }
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

    fn update_prune_index_from_version(&mut self, version: u64) -> Option<(usize, u64)> {
        // When suffix is empty, skip logic to update prune index.
        if self.messages.is_empty() {
            debug!("Skipped updating the prune_index for version ({version}), as suffix is empty.");
            return None;
        }

        let current_prune_index = self.get_meta().prune_index;

        let start_index = current_prune_index.unwrap_or(0);

        let end_index = match self.index_from_head(version) {
            Some(index) if index > start_index => index,
            _ => self.suffix_length() - 1,
        };

        debug!(
            "[Update prune index] Calculating prune index in suffix slice between index {start_index} <-> {end_index}. Current prune index version {current_prune_index:?}.",
        );

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

        Some((index, safe_prune_version))
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
