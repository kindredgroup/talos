use crate::{
    certifier::Outcome,
    core::{CandidateChannelMessage, CandidateMessageBaseTrait, DecisionChannelMessage},
    model::DecisionMessage,
    ChannelMessage,
};
use ahash::HashMap;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use uuid::Uuid;

pub fn get_default_headers() -> Value {
    json!({
        "headers": {
            "correlationId": "ef4d684b-cb42-4ff3-9bca-c462699c1672",
            "version": "1",
            "type": "SomeMockTypeA",
        },
    })
}

pub fn get_default_payload() -> Value {
    json!({
        "firstName": "abc",
        "lastName": "xyz",
        "address": "42 Wallaby Way, Sydney, New South Wales, Australia",
        "description": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor
                        incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip
                        ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.
                        Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."
    })
}

pub fn build_kafka_on_commit_message(topic: &str, key: &str, value: Option<Value>, headers: Option<Value>) -> Value {
    json!({
        "cluster": "",
        "headers": headers.unwrap_or_else(get_default_headers),
        "key": key,
        "keyEncoding": "",
        "partition": null,
        "topic": topic,
        "value": value.unwrap_or_else(get_default_payload),
        "valueEncoding": ""
    })
}

pub fn build_on_commit_publish_kafka_payload(kafka_payload: Vec<Value>) -> Value {
    json!({
        "publish": {
            "kafka": kafka_payload
        }
    })
}

/// Build mock outcome required for the decision.
///
/// - When the first parameter  is `Some(version)` - return `Aborted` with discord `Assertive`.
///
/// - When the first parameter is `None` and safepoint is `Some(version)` - return `Committed` with discord `Assertive`.
///
/// - Else - return `Aborted` with discord `Permissive`.
pub fn build_mock_outcome(conflict_version: Option<u64>, safepoint: Option<u64>) -> Outcome {
    if let Some(version) = conflict_version {
        return Outcome::Aborted {
            version: Some(version),
            discord: crate::certifier::Discord::Assertive,
        };
    }

    if let Some(vers) = safepoint {
        return Outcome::Commited {
            discord: crate::certifier::Discord::Assertive,
            safepoint: vers,
        };
    }

    Outcome::Aborted {
        version: None,
        discord: crate::certifier::Discord::Permissive,
    }
}

pub struct MockCandidatePayload {
    pub message: Value,
}

impl MockCandidatePayload {
    pub fn new() -> Self {
        MockCandidatePayload::default()
    }

    /// Add any field to the payload.
    fn add_field(&mut self, key: &str, value: &Value) {
        if let Some(message) = self.message.as_object_mut() {
            message.insert(key.to_string(), value.to_owned());
        }
    }

    pub fn add_statemap(mut self, statemap: &Value) -> Self {
        self.add_field("statemap", statemap);
        self
    }

    pub fn add_on_commit(mut self, on_commit: &Value) -> Self {
        self.add_field("onCommit", on_commit);
        self
    }

    pub fn build<T: DeserializeOwned>(self) -> T {
        serde_json::from_value(self.message).unwrap()
    }
}

impl Default for MockCandidatePayload {
    fn default() -> Self {
        let xid = Uuid::new_v4().to_string();
        let read_write_set = vec![Uuid::new_v4().to_string()];

        let message = json!({
            "xid": xid,
            "agent": "agent-test-payload",
            "cohort": "test-cohort",
            "readset": read_write_set,
            "readvers": [],
            "snapshot": 1,
            "writeset": read_write_set,
            "certificationStartedAt": 0,
            "requestCreatedAt": 0,
            "publishedAt": 0,
            "receivedAt": 0,
        });

        MockCandidatePayload { message }
    }
}
//

pub struct MockChannelMessage<T>
where
    T: CandidateMessageBaseTrait,
{
    candidate: T,
}

impl<T> MockChannelMessage<T>
where
    T: CandidateMessageBaseTrait + Clone,
{
    pub fn new(candidate: &T, version: u64) -> Self {
        let mut candidate = candidate.clone();
        candidate.add_version(version);
        Self { candidate }
    }

    pub fn build_candidate_channel_message(&self, headers: &HashMap<String, String>) -> ChannelMessage<T> {
        let channel_candidate = CandidateChannelMessage {
            message: self.candidate.clone(),
            headers: headers.clone(),
        };
        ChannelMessage::Candidate(channel_candidate.into())
    }

    pub fn build_decision_channel_message(
        &self,
        decision_version: u64,
        outcome: &Outcome,
        suffix_start: u64,
        headers: &HashMap<String, String>,
    ) -> ChannelMessage<T> {
        let decision = DecisionMessage::new(&self.candidate, outcome.clone(), suffix_start);

        let channel_decision = DecisionChannelMessage {
            decision_version,
            message: decision,
            headers: headers.clone(),
        };
        ChannelMessage::Decision(channel_decision.into())
    }
}
