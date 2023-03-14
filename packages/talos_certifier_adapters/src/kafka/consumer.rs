use std::time::Duration;

use async_trait::async_trait;
use log::{debug, info};
use rdkafka::{
    consumer::{Consumer, DefaultConsumerContext, StreamConsumer},
    Message, TopicPartitionList,
};
use talos_certifier::{
    core::{MessageVariant, SystemMonitorMessage},
    errors::SystemServiceError,
    model::{CandidateMessage, DecisionMessage},
    ports::{
        common::SharedPortTraits,
        errors::{MessageReceiverError, MessageReceiverErrorKind},
        MessageReciever,
    },
    ChannelMessage,
};
use tokio::sync::mpsc;

use crate::{kafka::utils::get_message_headers, KafkaAdapterError};

use super::{config::KafkaConfig, contexts::TalosKafkaConsumerContext, utils};

// Kafka Consumer Client
// #[derive(Debug, Clone)]
pub struct KafkaConsumer {
    pub consumer: StreamConsumer<DefaultConsumerContext>,
    pub topic: String,
    pub tpl: TopicPartitionList,
}

impl KafkaConsumer {
    pub fn new(config: &KafkaConfig) -> Self {
        let consumer = config.build_consumer_config().create().expect("Failed to create consumer");

        let topic = config.topic.clone();
        Self {
            consumer,
            topic,
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

    async fn consume_message(&mut self) -> Result<Option<Self::Message>, MessageReceiverError> {
        let message_received = self.consumer.recv().await.map_err(|e| MessageReceiverError {
            kind: MessageReceiverErrorKind::ReceiveError,
            version: None,
            reason: e.to_string(),
            data: None,
        })?;

        let partition = message_received.partition();

        let offset_i64 = message_received.offset();
        let offset = offset_i64 as u64;

        if offset == 0 {
            info!("Version zero message will be skipped");
            //TODO: should return some decision to receiver so that it aborts and notifies the cohorts.
            return Ok(None);
        }

        let headers = get_message_headers(&message_received).ok_or_else(|| MessageReceiverError {
            kind: MessageReceiverErrorKind::HeaderNotFound,
            version: Some(offset),
            reason: format!("Header not found for version={offset}"),
            data: Some("messageType".to_owned()),
        })?;

        let message_type = headers.get("messageType").ok_or_else(|| MessageReceiverError {
            kind: MessageReceiverErrorKind::HeaderNotFound,
            version: Some(offset),
            reason: format!("Header not found for version={offset}"),
            data: Some("messageType".to_owned()),
        })?;

        let raw_payload = message_received.payload().ok_or(MessageReceiverError {
            kind: MessageReceiverErrorKind::IncorrectData,
            version: Some(offset),
            reason: format!("Empty payload for version={offset}"),
            data: None,
        })?;

        let channel_msg = match utils::parse_message_variant(message_type).map_err(|e| MessageReceiverError {
            kind: MessageReceiverErrorKind::ParseError,
            version: Some(offset),
            reason: e.to_string(),
            data: Some(message_type.to_string()),
        })? {
            MessageVariant::Candidate => {
                let mut msg: CandidateMessage = utils::parse_kafka_payload(raw_payload).map_err(|e| MessageReceiverError {
                    kind: MessageReceiverErrorKind::ParseError,
                    version: Some(offset),
                    reason: e.to_string(),
                    data: Some(format!("{:?}", String::from_utf8_lossy(raw_payload))),
                })?;
                msg.version = offset;

                ChannelMessage::Candidate(msg)
            }
            MessageVariant::Decision => {
                let msg: DecisionMessage = utils::parse_kafka_payload(raw_payload).map_err(|e| MessageReceiverError {
                    kind: MessageReceiverErrorKind::ParseError,
                    version: Some(offset),
                    reason: e.to_string(),
                    data: Some(format!("{:?}", String::from_utf8_lossy(raw_payload))),
                })?;

                debug!("Decision received and the offset is {} !!!! ", offset);

                ChannelMessage::Decision(offset, msg)
            }
        };
        self.store_offsets(partition, offset_i64).map_err(|err| MessageReceiverError {
            kind: MessageReceiverErrorKind::SaveVersion,
            version: Some(offset),
            reason: err.to_string(),
            data: Some(format!("{}", offset)),
        })?;

        Ok(Some(channel_msg))
    }

    async fn subscribe(&self) -> Result<(), SystemServiceError> {
        self.consumer.subscribe(&[&self.topic]).map_err(|err| MessageReceiverError {
            kind: MessageReceiverErrorKind::SubscribeError,
            version: None,
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
                version: Some(vers),
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
