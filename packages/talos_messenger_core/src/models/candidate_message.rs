use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use talos_certifier::core::CandidateMessageBaseTrait;

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase", tag = "_typ")]
pub struct MessengerCandidateMessage {
    // UNIQUENESS FIELD
    pub xid: String,
    // DATA FIELDS
    pub agent: String,
    pub cohort: String,
    pub snapshot: u64,
    // #[serde(skip_deserializing)]
    // pub readset: Vec<String>,
    // #[serde(skip_deserializing)]
    // pub readvers: Vec<u64>,
    // #[serde(skip_deserializing)]
    // pub writeset: Vec<String>,
    #[serde(skip_deserializing)]
    pub version: u64,
    // OPTIONAL FIELDS
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_commit: Option<Box<Value>>,

    // #[serde(skip_deserializing)]
    // pub statemap: Option<Vec<HashMap<String, Value>>>,
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

impl CandidateMessageBaseTrait for MessengerCandidateMessage {
    fn get_xid(&self) -> &str {
        self.xid.as_str()
    }

    fn get_snapshot(&self) -> u64 {
        self.snapshot
    }

    fn get_version(&self) -> u64 {
        self.version
    }

    fn get_agent(&self) -> &str {
        &self.agent
    }

    fn get_cohort(&self) -> &str {
        &self.cohort
    }

    fn add_version(&mut self, version: u64) {
        self.version = version;
    }

    fn add_candidate_received_metric(&mut self, received_at: i128) {
        self.received_at = received_at;
    }
}

// // $coverage:ignore-start
#[cfg(test)]
mod tests {
    use serde_json::json;
    use talos_certifier::test_helpers::mocks::payload::{build_kafka_on_commit_message, build_on_commit_publish_kafka_payload};

    use crate::models::MessengerCandidateMessage;

    #[test]
    fn deserialise_using_defaults() {
        let mut kafka_vec = vec![];
        for _ in 0..1 {
            kafka_vec.push(build_kafka_on_commit_message("some-topic", "1234", None, None));
        }

        let on_commit = build_on_commit_publish_kafka_payload(kafka_vec);

        let json = json!({
            "xid": "1a5793f8-4ca3-420f-8e81-219e7b039e2a",
            "agent": "test-agent",
            "cohort": "test-cohort",
            "readset": [
                "c5893d97-30f6-446d-a349-1741f7eff599"
            ],
            "readvers": [],
            "snapshot": 2,
            "writeset": [
                "c5893d97-30f6-446d-a349-1741f7eff599"
            ],
            "statemap": [
                {
                    "SomeStateMap": {
                        "initialVersion": 0,
                        "newVersion": 20
                    }
                }
            ],
            "onCommit": on_commit,
            "certificationStartedAt": 0,
            "requestCreatedAt": 0,
            "publishedAt": 0,
            "receivedAt": 0,
        });

        let deserialised: MessengerCandidateMessage = serde_json::from_value(json).unwrap();
        // env_logger::init();
        // error!("deserialised candidate message: {deserialised:#?}");
        assert!(deserialised.on_commit.is_some());
    }
}
