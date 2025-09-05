use opentelemetry::metrics::{Counter, Meter};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::marker::PhantomData;
use talos_certifier::{model::DecisionMessageTrait, ports::MessageReciever, ChannelMessage};
use talos_common_utils::ResetVariantTrait;
use time::OffsetDateTime;
use tracing::{debug, warn};

use crate::{
    events::{EventTimingsMap, ReplicatorCandidateEvent, StatemapEvents},
    models::ReplicatorCandidateMessage,
};

use super::{
    suffix::{ReplicatorSuffixItemTrait, ReplicatorSuffixTrait},
    utils::get_statemap_from_suffix_items,
};

#[derive(Debug, Clone, PartialEq)]
pub enum StatemapQueueChannelMessage {
    Message((u64, Vec<StatemapItem>, StatemapEvents)),
    UpdateSnapshot,
}

impl ResetVariantTrait for StatemapQueueChannelMessage {
    const RESET_VARIANT: Self = StatemapQueueChannelMessage::UpdateSnapshot;
}

#[derive(Debug, Clone, PartialEq)]
pub enum StatemapInstallState {
    Awaiting,
    Inflight,
    Installed,
}
#[derive(Debug, Clone)]
pub struct StatemapInstallerHashmap {
    // enqueued at
    // pub timestamp: i128, // nanos
    pub statemaps: Vec<StatemapItem>,
    pub version: u64,
    pub safepoint: Option<u64>,
    pub state: StatemapInstallState,
    pub events: StatemapEvents,
}

#[derive(Debug)]
pub enum StatemapInstallationStatus {
    Success(u64),
    Error(u64, String),
}

#[derive(Debug, Clone)]
pub enum ReplicatorChannel {
    /// The last installed version, which will be used to update the prune_index in suffix
    /// to enable suffix pruning.
    LastInstalledVersion(u64),
    /// The last snapshot version that was updated to the persistance layer.
    SnapshotUpdatedVersion(u64),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CandidateDecisionOutcome {
    Committed,
    Aborted,
    Timedout,
    Undecided,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
pub struct StatemapItemWithEventTimings {
    pub statemap: StatemapItem,
    pub event_timings: EventTimingsMap,
}
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
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

#[derive(Default)]
struct Metrics {
    enabled: bool,
    c_candidates: Option<Counter<u64>>,
    c_dec_commits: Option<Counter<u64>>,
    c_dec_aborts: Option<Counter<u64>>,
}

impl Metrics {
    pub fn new(meter: Option<Meter>) -> Self {
        if let Some(meter) = meter {
            Self {
                enabled: true,
                c_candidates: Some(meter.u64_counter("repl_candidates").with_unit("tx").build()),
                c_dec_commits: Some(meter.u64_counter("repl_dec_commits").with_unit("tx").build()),
                c_dec_aborts: Some(meter.u64_counter("repl_dec_aborts").with_unit("tx").build()),
            }
        } else {
            Self {
                enabled: false,
                c_candidates: None,
                c_dec_commits: None,
                c_dec_aborts: None,
            }
        }
    }

    pub fn add_candidate(&self) {
        if let Some(c) = &self.c_candidates {
            c.add(1, &[]);
        }
    }
    pub fn add_decision(&self, outcome: CandidateDecisionOutcome) {
        if !self.enabled {
            return;
        }

        match outcome {
            CandidateDecisionOutcome::Committed => {
                let _ = self.c_dec_commits.as_ref().map(|m| m.add(1, &[]));
            }
            CandidateDecisionOutcome::Aborted => {
                let _ = self.c_dec_aborts.as_ref().map(|m| m.add(1, &[]));
            }
            _ => {}
        }
    }
}

pub struct Replicator<T, S, M>
where
    T: ReplicatorSuffixItemTrait,
    S: ReplicatorSuffixTrait<T> + std::fmt::Debug,
    M: MessageReciever<Message = ChannelMessage<ReplicatorCandidateMessage>> + Send + Sync,
{
    pub receiver: M,
    pub suffix: S,
    pub last_installing: u64,
    pub next_commit_offset: Option<u64>,
    metrics: Metrics,
    _phantom: PhantomData<T>,
}

impl<T, S, M> Replicator<T, S, M>
where
    T: ReplicatorSuffixItemTrait,
    S: ReplicatorSuffixTrait<T> + std::fmt::Debug,
    M: MessageReciever<Message = ChannelMessage<ReplicatorCandidateMessage>> + Send + Sync,
{
    pub fn new(receiver: M, suffix: S, meter: Option<Meter>) -> Self {
        Replicator {
            receiver,
            suffix,
            last_installing: 0,
            next_commit_offset: None,
            metrics: Metrics::new(meter),
            _phantom: PhantomData,
        }
    }

    pub(crate) async fn process_consumer_message(&mut self, version: u64, message: T) {
        if version > 0 {
            self.suffix.insert(version, message).unwrap();
            self.metrics.add_candidate();
            if let Some(suffix_item) = self.suffix.get_mut(version) {
                suffix_item.item.record_event_timestamp(
                    ReplicatorCandidateEvent::ReplicatorCandidateReceived,
                    OffsetDateTime::now_utc().unix_timestamp_nanos(),
                );
            }
        } else {
            warn!("Version 0 will not be inserted into suffix.")
        }
    }

    pub(crate) async fn process_decision_message<D: DecisionMessageTrait>(&mut self, decision_version: u64, decision_message: D) {
        let version = decision_message.get_candidate_version();

        let decision_outcome = match decision_message.get_decision() {
            talos_certifier::model::Decision::Committed => CandidateDecisionOutcome::Committed,
            talos_certifier::model::Decision::Aborted => CandidateDecisionOutcome::Aborted,
        };
        self.suffix.update_suffix_item_decision(version, decision_version).unwrap();
        self.suffix.set_decision_outcome(version, Some(decision_outcome.clone()));
        self.suffix.set_safepoint(version, decision_message.get_safepoint());

        if let Some(suffix_item) = self.suffix.get_mut(version) {
            suffix_item.item.record_event_timestamp(
                ReplicatorCandidateEvent::ReplicatorDecisionReceived,
                OffsetDateTime::now_utc().unix_timestamp_nanos(),
            );
        }

        // If this is a duplicate, we mark it as installed (assuming the original version always comes first and therefore that will be installed.)
        if decision_message.is_duplicate() {
            self.suffix.set_item_installed(version);
        }

        self.metrics.add_decision(decision_outcome);
    }

    // TODO: Instrument with span
    pub(crate) fn generate_statemap_batch(&mut self) -> Vec<(u64, Vec<StatemapItem>, EventTimingsMap)> {
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

    pub(crate) fn prepare_offset_for_commit(&mut self, version: u64) {
        debug!(
            "[prepare_offset_for_commit] Last installing version = {} | suffix head = {} | last_installed version ={:?}",
            self.last_installing,
            self.suffix.get_suffix_meta().head,
            version
        );
        if let Some(current_offset) = self.next_commit_offset {
            if version.gt(&current_offset) {
                self.next_commit_offset = Some(version);
                debug!("next_commit_offset moving from {:?} --> {} ", current_offset, version);
            }
        } else {
            self.next_commit_offset = Some(version);
        }
    }

    pub(crate) async fn commit(&mut self) {
        if let Some(version) = self.next_commit_offset {
            debug!("Committed till offset {:?} ", version);
            self.receiver.update_offset_to_commit(version as i64).unwrap();
            self.receiver.commit_async();
            // self.next_commit_offset = None;
        }
    }
}
