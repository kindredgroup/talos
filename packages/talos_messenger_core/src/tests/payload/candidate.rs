use ahash::HashMap;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use talos_certifier::{
    certifier::Outcome,
    core::{CandidateChannelMessage, DecisionChannelMessage},
    model::{CandidateMessage, DecisionMessage},
    ChannelMessage,
};
use uuid::Uuid;

pub struct CandidateTestPayload {
    pub message: Value,
}

impl CandidateTestPayload {
    pub fn new() -> Self {
        CandidateTestPayload::default()
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

impl Default for CandidateTestPayload {
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

        CandidateTestPayload { message }
    }
}
//

pub struct MockChannelMessage {
    candidate: CandidateMessage,
}

impl MockChannelMessage {
    pub fn new(candidate: &CandidateMessage, version: u64) -> Self {
        Self {
            candidate: CandidateMessage { version, ..candidate.clone() },
        }
    }

    pub fn build_candidate_channel_message(&self, headers: &HashMap<String, String>) -> ChannelMessage {
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
    ) -> ChannelMessage {
        let decision = DecisionMessage::new(&self.candidate, outcome.clone(), suffix_start);

        let channel_decision = DecisionChannelMessage {
            decision_version,
            message: decision,
            headers: headers.clone(),
        };
        ChannelMessage::Decision(channel_decision.into())
    }
}
