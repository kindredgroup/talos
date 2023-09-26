use log::warn;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::marker::PhantomData;
use talos_certifier::{model::DecisionMessageTrait, ports::MessageReciever, ChannelMessage};

use super::{
    suffix::{ReplicatorSuffixItemTrait, ReplicatorSuffixTrait},
    utils::get_statemap_from_suffix_items,
};

#[derive(Debug, Clone, PartialEq)]
pub enum StatemapInstallState {
    Awaiting,
    Inflight,
    Installed,
}
#[derive(Debug, Clone)]
pub struct StatemapInstallerHashmap {
    pub statemaps: Vec<StatemapItem>,
    pub version: u64,
    pub safepoint: Option<u64>,
    pub state: StatemapInstallState,
}

#[derive(Debug)]
pub enum StatemapInstallationStatus {
    Success(u64),
    Error(u64, String),
}

#[derive(Debug)]
pub enum ReplicatorChannel {
    InstallationSuccess(Vec<u64>),
    InstallationFailure(String),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CandidateDecisionOutcome {
    Committed,
    Aborted,
    Timedout,
    Undecided,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
pub struct StatemapItem {
    pub action: String,
    pub version: u64,
    pub safepoint: Option<u64>,
    pub payload: Value,
}

impl StatemapItem {
    pub fn new(action: String, version: u64, payload: Value, safepoint: Option<u64>) -> Self {
        StatemapItem {
            action,
            version,
            payload,
            safepoint,
        }
    }
}

pub struct Replicator<T, S, M>
where
    T: ReplicatorSuffixItemTrait,
    S: ReplicatorSuffixTrait<T> + std::fmt::Debug,
    M: MessageReciever<Message = ChannelMessage> + Send + Sync,
{
    pub receiver: M,
    pub suffix: S,
    pub last_installing: u64,
    pub next_commit_offset: Option<u64>,
    _phantom: PhantomData<T>,
}

impl<T, S, M> Replicator<T, S, M>
where
    T: ReplicatorSuffixItemTrait,
    S: ReplicatorSuffixTrait<T> + std::fmt::Debug,
    M: MessageReciever<Message = ChannelMessage> + Send + Sync,
{
    pub fn new(receiver: M, suffix: S) -> Self {
        Replicator {
            receiver,
            suffix,
            last_installing: 0,
            next_commit_offset: None,
            _phantom: PhantomData,
        }
    }

    pub(crate) async fn process_consumer_message(&mut self, version: u64, message: T) {
        if version > 0 {
            self.suffix.insert(version, message).unwrap();
        } else {
            warn!("Version 0 will not be inserted into suffix.")
        }
    }

    pub(crate) async fn process_decision_message<D: DecisionMessageTrait>(&mut self, decision_version: u64, decision_message: D) {
        let version = decision_message.get_candidate_version();

        let decision_outcome = match decision_message.get_decision() {
            talos_certifier::model::Decision::Committed => Some(CandidateDecisionOutcome::Committed),
            talos_certifier::model::Decision::Aborted => Some(CandidateDecisionOutcome::Aborted),
        };
        self.suffix.update_suffix_item_decision(version, decision_version).unwrap();
        self.suffix.set_decision_outcome(version, decision_outcome);
        self.suffix.set_safepoint(version, decision_message.get_safepoint());

        // If this is a duplicate, we mark it as installed (assuming the original version always comes first and therefore that will be installed.)
        if decision_message.is_duplicate() {
            self.suffix.set_item_installed(version);
        }
    }

    pub(crate) fn generate_statemap_batch(&mut self) -> Vec<(u64, Vec<StatemapItem>)> {
        // get batch of items from suffix to install.
        let items = self.suffix.get_message_batch_from_version(self.last_installing, None);

        let Some(last_item) = items.last() else {
            // We don't have to explicitly check the vec is empty since if items.last() returns `None`
            // we implicitly know the vec is empty.
            return vec![];
        };
        self.last_installing = last_item.item_ver;
        // generate the statemap from each item in batch.
        get_statemap_from_suffix_items(items.into_iter())
    }

    pub(crate) async fn prepare_offset_for_commit(&mut self) {
        if self.last_installing > 0 {
            if let Some(last_installed) = self.suffix.get_last_installed(Some(self.last_installing)) {
                let version = last_installed.decision_ver.unwrap();
                self.next_commit_offset = Some(version);
            }
        }
    }

    pub(crate) async fn commit(&mut self) {
        if let Some(version) = self.next_commit_offset {
            self.receiver.update_savepoint(version as i64).await.unwrap();
            self.receiver.commit_async();
            self.next_commit_offset = None;
        }
    }
}
