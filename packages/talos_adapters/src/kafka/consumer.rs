use std::{num::TryFromIntError, str::FromStr, time::Duration};

use async_trait::async_trait;
use log::{debug, info, trace};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    Message, TopicPartitionList,
};
use talos_core::{
    core::MessageVariant,
    errors::SystemServiceError,
    model::{candidate_message::CandidateMessage, decision_message::DecisionMessage},
    ports::{
        common::SharedPortTraits,
        errors::{MessageReceiverError, MessageReceiverErrorKind},
        message::MessageReciever,
    },
    ChannelMessage,
};

use crate::{kafka::utils::get_message_headers, KafkaAdapterError};

use super::{config::Config, utils};

// Kafka Consumer Client
// #[derive(Debug, Clone)]
pub struct KafkaConsumer {
    pub consumer: StreamConsumer,
    pub topic: String,
    pub tpl: TopicPartitionList,
}

impl KafkaConsumer {
    pub fn new(config: &Config) -> Self {
        //TODO :Error handling to be improved.
        let consumer = config.build_consumer_config().create().expect("Failed to create consumer");

        let topic = &utils::kafka_topic_prefixed(&config.consumer_topic, &config.topic_prefix);
        Self {
            consumer,
            topic: topic.to_string(),
            tpl: TopicPartitionList::new(),
        }
    }

    pub fn store_offsets(&mut self, partition: i32, offset: i64) -> Result<(), KafkaAdapterError> {
        debug!("Partition {partition} and Offset {offset}");

        let offset_to_update = offset + 1;

        if self.tpl.find_partition(&self.topic, partition).is_none() {
            self.tpl
                .add_partition_offset(&self.topic, partition, rdkafka::Offset::Offset(offset_to_update))
                .map_err(|e| KafkaAdapterError::Commit(e, Some(offset_to_update)))?;
        } else {
            self.tpl
                .set_partition_offset(&self.topic, partition, rdkafka::Offset::Offset(offset_to_update))
                .map_err(|e| KafkaAdapterError::Commit(e, Some(offset_to_update)))?;
        }
        Ok(())
    }
}

#[async_trait]
impl MessageReciever for KafkaConsumer {
    type Message = ChannelMessage;

    async fn consume_message(&mut self) -> Result<Option<Self::Message>, SystemServiceError> {
        let message_received = self.consumer.recv().await.map_err(|e| MessageReceiverError {
            kind: MessageReceiverErrorKind::ReceiveError,
            reason: e.to_string(),
            data: None,
        })?;

        let partition = message_received.partition();

        let headers = get_message_headers(&message_received).ok_or_else(|| MessageReceiverError {
            kind: MessageReceiverErrorKind::IncorrectData,
            reason: "Header not found".to_owned(),
            data: Some("messageType".to_owned()),
        })?;

        let offset_i64 = message_received.offset();
        let offset: u64 = offset_i64.try_into().map_err(|err: TryFromIntError| MessageReceiverError {
            kind: MessageReceiverErrorKind::ParseError,
            reason: format!("Error converting offset error={}", err),
            data: Some(format!("{}", offset_i64)),
        })?;

        if offset == 0 {
            info!("Version zero message will be skipped");
            return Ok(None);

            // Err(MessageReceiverError {
            //     kind: MessageReceiverErrorKind::VersionZero,
            //     reason: "Version zero message will be skipped".to_owned(),
            //     data: None,
            // })?;
        }

        let message_type = headers.get("messageType").ok_or_else(|| MessageReceiverError {
            kind: MessageReceiverErrorKind::IncorrectData,
            reason: "Header not found".to_owned(),
            data: Some("messageType".to_owned()),
        })?;

        let raw_payload = message_received.payload().ok_or(MessageReceiverError {
            kind: MessageReceiverErrorKind::IncorrectData,
            reason: "Empty payload".to_owned(),
            data: None,
        })?;
        let msg = String::from_utf8_lossy(raw_payload).into_owned();

        let kafka_msg_type = MessageVariant::from_str(message_type).map_err(|e| MessageReceiverError {
            kind: MessageReceiverErrorKind::ParseError,
            reason: format!("Error parsing message type error={}", e),
            data: Some(message_type.to_string()),
        })?;

        trace!("Message Received and Deserialized... {} for message type  {:#?} ", msg, kafka_msg_type);
        let channel_msg = match kafka_msg_type {
            MessageVariant::Candidate => {
                let mut msg: CandidateMessage = serde_json::from_slice(raw_payload).map_err(|err| MessageReceiverError {
                    kind: MessageReceiverErrorKind::ParseError,
                    reason: format!("Error parsing Candidate message error={}", err),
                    data: Some(msg.to_string()),
                })?;
                msg.version = offset;

                ChannelMessage::Candidate(msg)
            }
            MessageVariant::Decision => {
                let msg: DecisionMessage = serde_json::from_slice(raw_payload).map_err(|err| MessageReceiverError {
                    kind: MessageReceiverErrorKind::ParseError,
                    reason: format!("Error parsing Decision message error={}", err),
                    data: Some(msg.to_string()),
                })?;

                debug!("Decision received and the offset is {} !!!! ", offset);

                ChannelMessage::Decision(offset, msg)
            } // _ => Err(KafkaAdapterError::UnknownMessageType(message_type.to_string())),
        };
        self.store_offsets(partition, offset_i64).map_err(|err| MessageReceiverError {
            kind: MessageReceiverErrorKind::SaveVersion,
            reason: err.to_string(),
            data: Some(format!("{}", offset_i64)),
        })?;

        Ok(Some(channel_msg))
    }

    async fn subscribe(&self) -> Result<(), SystemServiceError> {
        self.consumer.subscribe(&[&self.topic]).map_err(|err| MessageReceiverError {
            kind: MessageReceiverErrorKind::SubscribeError,
            reason: err.to_string(),
            data: Some(self.topic.to_owned()),
        })?;
        Ok(())
    }

    async fn unsubscribe(&self) {
        self.consumer.unsubscribe();
    }

    async fn commit(&self, vers: u64) -> Result<(), SystemServiceError> {
        let vers_i64: i64 = vers.try_into().unwrap_or_default();
        self.consumer
            .commit(&self.tpl, rdkafka::consumer::CommitMode::Sync)
            .map_err(|err| MessageReceiverError {
                kind: MessageReceiverErrorKind::CommitError,
                reason: err.to_string(),
                data: Some(format!("{}", vers_i64)),
            })?;
        Ok(())
    }
}

#[async_trait]
impl SharedPortTraits for KafkaConsumer {
    async fn is_healthy(&self) -> bool {
        matches!(self.consumer.client().fetch_metadata(None, Duration::from_secs(2)), Ok(_result))
    }
    async fn shutdown(&self) -> bool {
        self.consumer.unsubscribe();
        true
    }
}
