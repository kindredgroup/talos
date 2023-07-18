use async_trait::async_trait;
use log::warn;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::marker::PhantomData;
use talos_certifier::{model::DecisionMessageTrait, ports::MessageReciever, ChannelMessage};

use super::{
    suffix::{ReplicatorSuffixItemTrait, ReplicatorSuffixTrait},
    utils::{get_filtered_batch, get_statemap_from_suffix_items},
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
    GaveUp(u64),
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

pub type RetryCount = u32;
pub enum ReplicatorInstallStatus {
    Success,
    Gaveup(RetryCount),
}

#[async_trait]
pub trait ReplicatorInstaller {
    async fn install(&self, sm: Vec<StatemapItem>, version: Option<u64>) -> Result<ReplicatorInstallStatus, String>;
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

    pub(crate) fn generate_statemap_batch(&mut self) -> (Vec<u64>, Vec<(u64, Vec<StatemapItem>)>) {
        // let instance = OffsetDateTime::now_utc().unix_timestamp_nanos();
        // let msg_batch_instance = Instant::now();
        // get batch of items from suffix to install.
        let items_option = self.suffix.get_message_batch_from_version(self.last_installing, None);
        // let msg_batch_instance_elapsed = msg_batch_instance.elapsed();

        let mut statemaps_batch = vec![];

        // #[allow(unused_assignments)]
        // let mut msg_statemap_create_elapsed = Duration::from_nanos(0);
        let mut all_version_picked: Vec<u64> = vec![];

        if let Some(items) = items_option {
            all_version_picked = items.iter().map(|i| i.item_ver).collect();
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
        (all_version_picked, statemaps_batch)
    }

    pub(crate) async fn commit_till_last_installed(&mut self) {
        if self.last_installing > 0 {
            if let Some(last_installed) = self.suffix.get_last_installed(Some(self.last_installing)) {
                let version = last_installed.decision_ver.unwrap();
                self.receiver.update_savepoint(version as i64).await.unwrap();

                self.receiver.commit().await.unwrap();
            }
        }
    }
}
