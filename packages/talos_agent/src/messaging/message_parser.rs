use std::collections::HashMap;

use rdkafka::message::BorrowedMessage;
use rdkafka::message::Headers;
use rdkafka::Message;
use time::OffsetDateTime;

use crate::{
    api::TalosType,
    messaging::api::{HEADER_AGENT_ID, HEADER_MESSAGE_TYPE},
};

use super::{
    api::{CandidateMessage, Decision, DecisionMessage, ReceivedMessage, TalosMessageType, TxProcessingTimeline},
    errors::MessagingError,
};

pub(crate) struct MessageReceiver<'a, L: MessageListener> {
    agent_name: &'a str,
    headers: HashMap<String, String>,
    pub(crate) is_id_matching: bool,
    pub(crate) is_decision: bool,
    raw_message: &'a BorrowedMessage<'a>,
    talos_type: TalosType,
    pub(crate) offset: u64,
    message_listener: Option<&'a L>,
}

pub(crate) trait MessageListener {
    fn on_candidate(&self, message: &ReceivedMessage);
    fn on_decision(&self, message: &ReceivedMessage);
}

// pub(crate) type OnCandidate = Box<dyn Fn(&ReceivedMessage)>;

impl<'a, L: MessageListener> MessageReceiver<'a, L> {
    pub(crate) fn new(agent_name: &'a str, talos_type: TalosType, raw_message: &'a BorrowedMessage) -> Self {
        Self {
            agent_name,
            headers: HashMap::<String, String>::new(),
            is_id_matching: false,
            is_decision: false,
            raw_message,
            talos_type,
            offset: raw_message.offset() as u64,
            message_listener: None,
        }
    }

    pub(crate) fn add_listener(&mut self, listener: &'a L) {
        self.message_listener = Some(listener);
    }

    pub(crate) fn read_headers(&mut self) -> Result<(), MessagingError> {
        if let Some(headers) = self.raw_message.headers() {
            let mut parsed_message_type: Option<TalosMessageType> = None;
            for header in headers.iter() {
                if let Some(v) = header.value {
                    let key = header.key.to_string();
                    let value = String::from_utf8_lossy(v).to_string();

                    if key == HEADER_AGENT_ID {
                        self.is_id_matching = value == self.agent_name;
                    } else if key == HEADER_MESSAGE_TYPE {
                        let message_type = TalosMessageType::try_from(value.as_str()).map_err(|parse_error| {
                            MessagingError::corrupted(
                                format!(
                                    "Unknown header value {}='{}', skipping this message: {}.",
                                    HEADER_MESSAGE_TYPE, value, self.offset
                                ),
                                Some(parse_error.to_string()),
                            )
                        })?;
                        self.is_decision = message_type == TalosMessageType::Decision;
                        parsed_message_type = Some(message_type);
                    }

                    self.headers.insert(key, value);
                }
            }

            if parsed_message_type.is_none() {
                return Err(MessagingError::corrupted(
                    format!("There is no header '{}', skipping this message: {}.", HEADER_MESSAGE_TYPE, self.offset),
                    None,
                ));
            }
        } else {
            return Err(MessagingError::corrupted(
                format!("There are no headers in this message: {}, offset", self.offset),
                None,
            ));
        }

        Ok(())
    }

    pub(crate) fn read_content(self) -> Result<ReceivedMessage, MessagingError> {
        if self.is_decision && !self.is_id_matching {
            let received_decision = ReceivedMessage::new_decision_for_another_agent(self.offset);
            let _ = self.message_listener.map(|handler| handler.on_decision(&received_decision));
            return Ok(received_decision);
        }

        let data = self.raw_message.payload_view::<str>();
        if data.is_none() {
            return Err(MessagingError::corrupted(format!("There is no body in the message: {}", self.offset), None));
        }

        let raw_data = data.unwrap();
        let json_as_text =
            raw_data.map_err(|utf_error| MessagingError::new_corrupted_payload("Payload is not UTF8 text".to_string(), utf_error.to_string()))?;

        let message = if self.is_decision {
            // convert JSON text into DecisionMessage
            let decision = serde_json::from_str::<DecisionMessage>(json_as_text)
                .map_err(|json_error| MessagingError::new_corrupted_payload("Payload is not JSON text".to_string(), json_error.to_string()))?;
            let received_decision = ReceivedMessage::new_decision(decision, self.offset, self.headers);
            let _ = self.message_listener.map(|handler| handler.on_decision(&received_decision));
            received_decision
        } else {
            // incoming candidate message has a special case for parsing, we need to check the agent config and maybe generate synthetic decision
            match self.talos_type {
                TalosType::External => {
                    // treat candidate as normal candidate

                    let received_candidate = ReceivedMessage::new_candidate(self.offset);
                    let _ = self.message_listener.map(|handler| handler.on_candidate(&received_candidate));
                    received_candidate
                }
                TalosType::InProcessMock => {
                    // treat candidate as decision
                    let decision = Self::parse_payload_as_candidate(json_as_text, Decision::Committed)?;
                    let received_decision = ReceivedMessage::new_decision(decision, self.offset, self.headers);
                    let _ = self.message_listener.map(|handler| handler.on_decision(&received_decision));
                    received_decision
                }
            }
        };

        Ok(message)
    }

    fn parse_payload_as_candidate(json_as_text: &str, decision: Decision) -> Result<DecisionMessage, MessagingError> {
        let now = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let metrics = TxProcessingTimeline {
            candidate_received: now,
            decision_created_at: now,
            ..Default::default()
        };

        // convert JSON text into DecisionMessage
        serde_json::from_str::<CandidateMessage>(json_as_text)
            .map_err(|json_error| MessagingError::new_corrupted_payload("Payload is not JSON text".to_string(), json_error.to_string()))
            .map(|candidate| DecisionMessage {
                xid: candidate.xid,
                agent: candidate.agent,
                cohort: candidate.cohort,
                decision,
                suffix_start: 0,
                version: 0,
                safepoint: None,
                conflicts: None,
                metrics: Some(metrics),
            })
    }
}
