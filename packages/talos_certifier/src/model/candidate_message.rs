use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::certifier::CertifierCandidate;

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
    pub on_commit: Option<Box<Value>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub statemap: Option<Vec<HashMap<String, Value>>>,

    /// Cohort started certification
    #[serde(default)]
    pub certification_started_at: i128,
    /// The request for certification is created
    #[serde(default)]
    pub request_created_at: i128,
    /// Candidate published to kafka (agent time)
    pub published_at: i128,
    #[serde(skip_deserializing)]
    /// Candidate received by Certifier
    pub received_at: i128,
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

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use crate::model::CandidateMessage;

    #[test]
    fn deserialise_using_defaults() {
        let json = r#"{
            "xid": "xid-1",
            "agent": "agent",
            "cohort": "cohort",
            "snapshot": 2,
            "readset": [ "3", "4" ],
            "readvers": [ 5, 5 ],
            "writeset": [ "6" ],
            "publishedAt": 1
        }"#;

        let deserialised: CandidateMessage = serde_json::from_str(json).unwrap();
        assert_eq!(deserialised.certification_started_at, 0);
        assert_eq!(deserialised.request_created_at, 0);
        assert_eq!(deserialised.published_at, 1);
        assert_eq!(deserialised.xid, String::from("xid-1"));
        assert_eq!(deserialised.agent, String::from("agent"));
        assert_eq!(deserialised.cohort, String::from("cohort"));
        assert_eq!(deserialised.snapshot, 2);
        assert_eq!(deserialised.readset, vec!(String::from("3"), String::from("4")));
        assert_eq!(deserialised.readvers, vec!(5, 5));
        assert_eq!(deserialised.writeset, vec!(String::from("6")));
    }
}
