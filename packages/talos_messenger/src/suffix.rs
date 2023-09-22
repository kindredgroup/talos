use ahash::{HashMap, HashMapExt};
use log::error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::Debug;
use talos_certifier::model::{CandidateMessage, Decision, DecisionMessageTrait};
use talos_suffix::{core::SuffixMeta, Suffix, SuffixItem, SuffixTrait};

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
    //TODO: GK - Mark as error?
    /// When there is an error?
    // Error(String),
    /// When all commit action has are completed.
    Processed,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct FilteredCommitAction {
    pub payload: Value,
    pub count: u32,
    pub is_completed: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct CommitActionsWithVersion {
    pub actions: HashMap<String, FilteredCommitAction>,
    pub version: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct MessengerCandidate {
    pub candidate: CandidateMessage,

    pub safepoint: Option<u64>,

    pub decision: Option<Decision>,

    pub state: SuffixItemState,

    pub commit_actions: HashMap<String, FilteredCommitAction>,
}

impl From<CandidateMessage> for MessengerCandidate {
    fn from(candidate: CandidateMessage) -> Self {
        MessengerCandidate {
            candidate,
            safepoint: None,
            decision: None,

            state: SuffixItemState::AwaitingDecision,
            commit_actions: HashMap::new(),
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

    fn set_commit_action(&mut self, commit_actions: HashMap<String, FilteredCommitAction>) {
        self.commit_actions = commit_actions
    }

    fn get_state(&self) -> &SuffixItemState {
        &self.state
    }

    fn get_safepoint(&self) -> &Option<u64> {
        &self.safepoint
    }
    fn get_commit_actions(&self) -> &HashMap<String, FilteredCommitAction> {
        &self.commit_actions
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
    fn set_commit_action(&mut self, commit_actions: HashMap<String, FilteredCommitAction>);
    //  Getters
    fn get_state(&self) -> &SuffixItemState;
    fn get_safepoint(&self) -> &Option<u64>;
    fn get_commit_actions(&self) -> &HashMap<String, FilteredCommitAction>;

    fn is_abort(&self) -> Option<bool>;
}

pub trait MessengerSuffixTrait<T: MessengerSuffixItemTrait>: SuffixTrait<T> {
    //  Setters
    fn set_item_state(&mut self, version: u64, process_state: SuffixItemState);

    //  Getters
    fn get_mut(&mut self, version: u64) -> Option<&mut SuffixItem<T>>;
    fn get_item_state(&self, version: u64) -> Option<SuffixItemState>;
    fn get_last_installed(&self, to_version: Option<u64>) -> Option<&SuffixItem<T>>;
    // fn update_suffix_item_decision(&mut self, version: u64, decision_ver: u64) -> SuffixResult<()>;
    fn get_suffix_meta(&self) -> &SuffixMeta;
    fn installed_all_prior_decided_items(&self, version: u64) -> bool;

    fn get_suffix_items_to_process(&self) -> Vec<CommitActionsWithVersion>;
    // updates
    fn update_prune_index(&mut self, version: u64);
    fn update_item_decision<D: DecisionMessageTrait>(&mut self, version: u64, decision_version: u64, decision_message: &D);
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

    fn get_item_state(&self, version: u64) -> Option<SuffixItemState> {
        if let Ok(Some(suffix_item)) = self.get(version) {
            Some(suffix_item.item.get_state().clone())
        } else {
            None
        }
    }

    fn get_last_installed(&self, _to_version: Option<u64>) -> Option<&SuffixItem<T>> {
        todo!()
    }

    // TODO: GK - Elevate this to core suffix
    fn get_suffix_meta(&self) -> &SuffixMeta {
        &self.meta
    }

    fn installed_all_prior_decided_items(&self, _version: u64) -> bool {
        todo!()
    }

    fn get_suffix_items_to_process(&self) -> Vec<CommitActionsWithVersion> {
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
            .map(|x| CommitActionsWithVersion {
                version: x.item_ver,
                actions: x.item.get_commit_actions().clone(),
            })
            .collect();

        items
    }

    fn update_prune_index(&mut self, _version: u64) {
        todo!()
    }

    fn update_item_decision<D: DecisionMessageTrait>(&mut self, version: u64, decision_version: u64, decision_message: &D) {
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
        }
    }
}
