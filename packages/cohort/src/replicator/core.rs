use async_trait::async_trait;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, io::Error, marker::PhantomData, sync::Arc};
use talos_certifier::{
    model::{CandidateMessage, DecisionMessageTrait},
    ports::MessageReciever,
    ChannelMessage,
};

use crate::{
    replicator::utils::{get_filtered_batch, get_statemap_from_suffix_items},
    state::postgres::{data_access::PostgresApi, database::Database},
};

use super::{
    suffix::{ReplicatorSuffixItemTrait, ReplicatorSuffixTrait},
    utils::statemap_install_handler,
};

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
    async fn install(&self, sm: Vec<StatemapItem>, version: Option<u64>) -> Result<bool, Error>;
}

pub struct ReplicatorStatemapInstaller {
    pub db: Arc<Database>,
}

#[async_trait]
impl ReplicatorInstaller for ReplicatorStatemapInstaller {
    async fn install(&self, sm: Vec<StatemapItem>, version: Option<u64>) -> Result<bool, Error> {
        let db = Arc::clone(&self.db);
        let mut manual_tx_api = PostgresApi { client: db.get().await };

        statemap_install_handler(sm, &mut manual_tx_api, version).await
    }
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

    pub(crate) fn generate_statemap_batch(&self) -> (Option<Vec<StatemapItem>>, Option<u64>) {
        let Some(batch) = self.suffix.get_message_batch(Some(1)) else {
            return (None, None);
        };

        let version = batch.last().unwrap().item_ver;

        // Filtering out messages that are not applicable.
        let filtered_message_batch = get_filtered_batch(batch.iter().copied());

        // Create the statemap batch
        let statemap_batch = get_statemap_from_suffix_items(filtered_message_batch);

        info!("Statemap_Batch={statemap_batch:#?} ");

        (Some(statemap_batch), Some(version))
    }
}
