use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::certifier::CertifierCandidate;

use super::delivery_order::DeliveryOrder;

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase", tag = "_typ")]
pub struct CandidateMessage {
    // UNIQUENESS FIELD
    pub xid: String,
    // DATA FIELDS
    pub agent: String,
    pub cohort: String,
    pub snapshot: u64,
    pub readset: Vec<String>,
    pub readvers: Vec<u64>,
    pub writeset: Vec<String>,

    #[serde(skip_deserializing)]
    pub version: u64,
    // OPTIONAL FIELDS
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_commit: Option<HashMap<String, Vec<DeliveryOrder>>>,
}

pub trait CandidateReadWriteSet {
    fn get_readset(&self) -> &Vec<String>;
    fn get_writeset(&self) -> &Vec<String>;
}

impl CandidateReadWriteSet for CandidateMessage {
    fn get_readset(&self) -> &Vec<String> {
        &self.readset
    }

    fn get_writeset(&self) -> &Vec<String> {
        &self.writeset
    }
}

impl CandidateMessage {
    pub fn convert_into_certifier_candidate(&self, version: u64) -> CertifierCandidate {
        CertifierCandidate {
            vers: version,
            snapshot: self.snapshot.to_owned(),
            readvers: self.readvers.to_owned(),
            readset: self.readset.to_owned(),
            writeset: self.writeset.to_owned(),
        }
    }
}
