use async_trait::async_trait;
use log::warn;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    io::Error,
    marker::PhantomData,
    time::{Duration, Instant},
};
use talos_certifier::{
    model::{CandidateMessage, DecisionMessageTrait},
    ports::MessageReciever,
    ChannelMessage,
};
use time::OffsetDateTime;

use super::{
    suffix::{ReplicatorSuffixItemTrait, ReplicatorSuffixTrait},
    utils::{get_filtered_batch, get_statemap_from_suffix_items},
};

#[derive(Debug)]
pub enum ReplicatorChannel {
    InstallationSuccess(Vec<u64>),
    // InstallationFailure(String),
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
    pub payload: Value,
}

impl StatemapItem {
    pub fn new(action: String, version: u64, payload: Value) -> Self {
        StatemapItem { action, version, payload }
    }
}

#[async_trait]
pub trait ReplicatorInstaller {
    async fn install(&mut self, sm: Vec<StatemapItem>, version: Option<u64>) -> Result<bool, Error>;
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct ReplicatorCandidate {
    pub candidate: CandidateMessage,

    #[serde(skip_deserializing)]
    pub safepoint: Option<u64>,

    #[serde(skip_deserializing)]
    pub decision_outcome: Option<CandidateDecisionOutcome>,

    #[serde(skip_deserializing)]
    pub is_installed: bool,
}

impl From<CandidateMessage> for ReplicatorCandidate {
    fn from(value: CandidateMessage) -> Self {
        ReplicatorCandidate {
            candidate: value,
            safepoint: None,
            decision_outcome: None,
            is_installed: false,
        }
    }
}

impl ReplicatorSuffixItemTrait for ReplicatorCandidate {
    fn get_safepoint(&self) -> &Option<u64> {
        &self.safepoint
    }
    fn get_statemap(&self) -> &Option<Vec<HashMap<String, Value>>> {
        &self.candidate.statemap
    }
    fn set_safepoint(&mut self, safepoint: Option<u64>) {
        self.safepoint = safepoint
    }
    fn set_decision_outcome(&mut self, decision_outcome: Option<CandidateDecisionOutcome>) {
        self.decision_outcome = decision_outcome
    }
    fn set_suffix_item_installed(&mut self) {
        self.is_installed = true
    }
    fn is_installed(&self) -> bool {
        self.is_installed
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
        // let instance = OffsetDateTime::now_utc().unix_timestamp_nanos();
        // let msg_batch_instance = Instant::now();
        // get batch of items from suffix to install.
        let items_option = self.suffix.get_message_batch_from_version(self.last_installing, None);
        // let msg_batch_instance_elapsed = msg_batch_instance.elapsed();

        let mut statemaps_batch = vec![];

        // #[allow(unused_assignments)]
        // let mut msg_statemap_create_elapsed = Duration::from_nanos(0);

        if let Some(items) = items_option {
            // let msg_batch_instance_filter = Instant::now();
            let filtered_message_batch = get_filtered_batch(items.iter().copied());
            // let msg_batch_instance_filter_elapsed = msg_batch_instance_filter.elapsed();

            // let msg_statemap_create = Instant::now();
            // generate the statemap from each item in batch.
            statemaps_batch = get_statemap_from_suffix_items(filtered_message_batch);

            // msg_statemap_create_elapsed = msg_statemap_create.elapsed();

            // let elapsed = OffsetDateTime::now_utc().unix_timestamp_nanos() - instance;

            if let Some(last_item) = items.last() {
                self.last_installing = last_item.item_ver;
            }

            // TODO: Remove TEMP_CODE and replace with proper metrics from feature/cohort-db-mock
            // if !items.is_empty() {
            //     let first_version = items.first().unwrap().item_ver;
            //     let last_version = items.last().unwrap().item_ver;
            //     debug!("[CREATE_STATEMAP] Processed total of count={} from_version={first_version:?} to_version={last_version:?} with batch_create_time={:?}, filter_time={msg_batch_instance_filter_elapsed:?}  statemap_create_time={msg_statemap_create_elapsed:?} and total_time={elapsed:?} {}", items.len(), msg_batch_instance_elapsed, elapsed);
            // };
        }
        statemaps_batch
    }
}
