use log::{error, warn};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{value::RawValue, Value};
use std::collections::HashMap;
use talos_certifier::core::CandidateMessageBaseTrait;

pub type OnCommitActions = HashMap<String, HashMap<String, Vec<Box<RawValue>>>>;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase", tag = "_typ")]
pub struct MessengerCandidateMessage {
    // UNIQUENESS FIELD
    pub xid: String,
    // DATA FIELDS
    pub agent: String,
    pub cohort: String,
    pub snapshot: u64,
    #[serde(skip_deserializing)]
    pub version: u64,
    // OPTIONAL FIELDS
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default, deserialize_with = "deserialize_oncommit_actions")]
    pub on_commit: Option<OnCommitActions>,
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

fn deserialize_oncommit_actions<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<OnCommitActions>, D::Error> {
    let val = Value::deserialize(deserializer)?;

    let final_deserialized_output = match val {
        Value::Null => Ok(None),
        Value::Object(map) => {
            let mut result = HashMap::new();
            for (k1, v1) in map {
                if let Value::Object(inner_map) = v1 {
                    let mut inner_result = HashMap::new();
                    for (k2, v2) in inner_map {
                        if let Ok(raw_value) = serde_json::from_value(v2) {
                            inner_result.insert(k2, raw_value);
                        } else {
                            warn!("Failed to deserialise on_commit action. Inner value is not deserializable to Vec<Box<RawValue>>");
                            return Ok(None); // If any inner value fails, return None.
                        }
                    }
                    result.insert(k1, inner_result);
                } else {
                    warn!("Failed to deserialise as on_commit action. Outer value is not Object type.");
                    return Ok(None); // If any outer value is not an object, return None.
                }
            }
            Ok(Some(result))
        }
        // Return None for any other type
        _ => {
            warn!("Failed to deserialise as on_commit action as it is not of the expected structure. Received on_commit_actions = {val:?}");
            Ok(None)
        }
    };

    final_deserialized_output
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
    #[test]
    fn deserialise_invalid_on_commit_action() {
        let on_commit = json!("Test");

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
        assert!(deserialised.on_commit.is_none());
    }
    #[test]
    fn deserialise_no_on_commit_action() {
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
            "certificationStartedAt": 0,
            "requestCreatedAt": 0,
            "publishedAt": 0,
            "receivedAt": 0,
        });
        let deserialised: MessengerCandidateMessage = serde_json::from_value(json).unwrap();
        assert!(deserialised.on_commit.is_none());
    }
}
