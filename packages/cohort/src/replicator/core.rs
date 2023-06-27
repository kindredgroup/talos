use async_trait::async_trait;
use log::warn;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, default, io::Error, marker::PhantomData};
use talos_certifier::{
    model::{CandidateMessage, DecisionMessageTrait},
    ports::MessageReciever,
    ChannelMessage,
};

use super::suffix::{ReplicatorSuffixItemTrait, ReplicatorSuffixTrait};

fn time_diff(time1_ns: i128, time2_ns: i128) -> (f64, String) {
    let diff = time2_ns - time1_ns;

    // seconds
    if diff >= 1_000_000_000 {
        let diff_seconds = diff as f64 / 1_000_000_000_f64;
        return (diff_seconds, format!("{diff_seconds:?}s"));
    }

    // ms
    if diff >= 1_000_000 {
        let diff_milliseconds = diff as f64 / 1_000_000_f64;
        return (diff_milliseconds, format!("{diff_milliseconds:?}ms"));
    }

    //μs
    if diff >= 1_000 {
        let diff_microseconds = diff as f64 / 1_000_f64;
        return (diff_microseconds, format!("{diff_microseconds:?}μs"));
    }

    (diff as f64, format!("{diff:?}ns"))
}

#[derive(Debug, Clone)]
pub struct ReplicatorInstallerMetricDuration {
    pub wait_in_channel_ms: String,
    pub statemap_installation_ms: String,
    pub post_install_suffix_updates_ms: String,
    pub suffix_prune_ms: String,
    pub total_ms: String,
}
#[derive(Debug, Clone)]
pub struct ReplicatorInstallerMetric {
    pub replicator_start_time: i128,
    pub channel_send_time: i128,
    pub channel_receive_time: i128,
    pub installation_start_time: i128,
    pub installation_end_time: i128,
    pub post_install_suffix_update_start: i128,
    pub post_install_suffix_update_end: i128,
    pub prune_start_time: i128,
    pub prune_end_time: i128,
}

impl ReplicatorInstallerMetric {
    pub fn calculate_durations(&self) -> ReplicatorInstallerMetricDuration {
        let has_prune = self.prune_start_time > 0;

        let replicator_end_time = if has_prune {
            self.prune_end_time
        } else {
            self.post_install_suffix_update_end
        };
        ReplicatorInstallerMetricDuration {
            wait_in_channel_ms: time_diff(self.channel_send_time, self.channel_receive_time).1,
            statemap_installation_ms: time_diff(self.installation_start_time, self.installation_end_time).1,
            post_install_suffix_updates_ms: time_diff(self.post_install_suffix_update_start, self.post_install_suffix_update_start).1,
            suffix_prune_ms: if has_prune {
                time_diff(self.prune_start_time, self.prune_end_time).1
            } else {
                (self.prune_start_time as f64).to_string()
            },
            total_ms: time_diff(self.replicator_start_time, replicator_end_time).1,
        }
    }
}
#[derive(Debug, Clone, Default)]
pub struct ReplicatorInstallerMetricBuilder {
    replicator_start_time: Option<i128>,
    channel_send_time: Option<i128>,
    channel_receive_time: Option<i128>,
    installation_start_time: Option<i128>,
    installation_end_time: Option<i128>,
    post_install_suffix_update_start: Option<i128>,
    post_install_suffix_update_end: Option<i128>,
    prune_start_time: Option<i128>,
    prune_end_time: Option<i128>,
}

impl ReplicatorInstallerMetricBuilder {
    pub fn new() -> Self {
        ReplicatorInstallerMetricBuilder::default()
    }

    pub fn add_replicator_start_time(mut self, time: i128) -> Self {
        self.replicator_start_time = Some(time);
        self
    }
    pub fn add_channel_send_time(mut self, time: i128) -> Self {
        self.channel_send_time = Some(time);
        self
    }
    pub fn add_channel_receive_time(mut self, time: i128) -> Self {
        self.channel_receive_time = Some(time);
        self
    }
    pub fn add_installation_start_time(mut self, time: i128) -> Self {
        self.installation_start_time = Some(time);
        self
    }
    pub fn add_installation_end_time(mut self, time: i128) -> Self {
        self.installation_end_time = Some(time);
        self
    }
    pub fn capture_post_install_suffix_update_start(&mut self, time: i128) {
        self.post_install_suffix_update_start = Some(time);
    }
    pub fn capture_post_install_suffix_update_end(&mut self, time: i128) {
        self.post_install_suffix_update_end = Some(time);
    }
    pub fn capture_prune_start_time(&mut self, time: i128) {
        self.prune_start_time = Some(time);
    }
    pub fn capture_prune_end_time(&mut self, time: i128) {
        self.prune_end_time = Some(time);
    }

    pub fn build(self) -> ReplicatorInstallerMetric {
        ReplicatorInstallerMetric {
            replicator_start_time: self.replicator_start_time.unwrap(),
            channel_send_time: self.channel_send_time.unwrap(),
            channel_receive_time: self.channel_receive_time.unwrap(),
            installation_start_time: self.installation_start_time.unwrap(),
            installation_end_time: self.installation_end_time.unwrap(),
            post_install_suffix_update_start: self.post_install_suffix_update_start.unwrap(),
            post_install_suffix_update_end: self.post_install_suffix_update_end.unwrap(),
            prune_start_time: self.prune_start_time.unwrap_or(0),
            prune_end_time: self.prune_end_time.unwrap_or(0),
        }
    }
}

#[derive(Debug)]
pub enum ReplicatorChannel {
    InstallationSuccess(Vec<u64>, ReplicatorInstallerMetricBuilder),
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
    pub last_installed: u64,
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
            last_installed: 0,
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

    // pub(crate) fn generate_statemap_batch(&self) -> (Option<Vec<StatemapItem>>, Option<u64>) {
    //     let Some(batch) = self.suffix.get_message_batch(Some(1)) else {
    //         return (None, None);
    //     };

    //     let version = batch.last().unwrap().item_ver;

    //     // Filtering out messages that are not applicable.
    //     let filtered_message_batch = get_filtered_batch(batch.iter().copied());

    //     // Create the statemap batch
    //     let statemap_batch = get_statemap_from_suffix_items(filtered_message_batch);

    //     info!("Statemap_Batch={statemap_batch:#?} ");

    //     (Some(statemap_batch), Some(version))
    // }
}
