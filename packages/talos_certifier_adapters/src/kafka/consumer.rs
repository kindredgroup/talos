use std::time::Duration;

use async_trait::async_trait;
use log::debug;
use rdkafka::{
    consumer::{Consumer, DefaultConsumerContext, StreamConsumer},
    Message, TopicPartitionList,
};
use talos_certifier::{
    core::MessageVariant,
    errors::SystemServiceError,
    model::{CandidateMessage, DecisionMessage},
    ports::{
        common::SharedPortTraits,
        errors::{MessageReceiverError, MessageReceiverErrorKind},
        MessageReciever,
    },
    ChannelMessage,
};
use talos_rdkafka_utils::kafka_config::KafkaConfig;
use time::OffsetDateTime;

use crate::{kafka::utils::get_message_headers, KafkaAdapterError};

use super::utils;

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

        match self.tpl.find_partition(&self.topic, partition) {
            Some(tpl) => {
                let offset_in_tpl = tpl.offset().to_raw().unwrap_or_default();

                // error!("Offset received ={offset} and offset in tpl ={offset_in_tpl}");
                if offset_to_update > offset_in_tpl {
                    // error!("Updating partition offset....");
                    self.tpl
                        .set_partition_offset(&self.topic, partition, rdkafka::Offset::Offset(offset_to_update))
                        .map_err(|e| KafkaAdapterError::Commit(e, Some(offset_to_update)))?;
                }
            }
            None => {
                self.tpl
                    .add_partition_offset(&self.topic, partition, rdkafka::Offset::Offset(offset_to_update))
                    .map_err(|e| KafkaAdapterError::Commit(e, Some(offset_to_update)))?;
            }
        };

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
                msg.received_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
                ChannelMessage::Candidate(msg)
            }
            MessageVariant::Decision => {
                let mut msg: DecisionMessage = utils::parse_kafka_payload(raw_payload).map_err(|e| MessageReceiverError {
                    kind: MessageReceiverErrorKind::ParseError,
                    version: Some(offset),
                    reason: e.to_string(),
                    data: Some(format!("{:?}", String::from_utf8_lossy(raw_payload))),
                })?;
                msg.metrics.decision_received_at = OffsetDateTime::now_utc().unix_timestamp_nanos();

                debug!("Decision received and the offset is {} !!!! ", offset);

                let tpl = self.tpl.elements_for_topic(&self.topic);
                if tpl.is_empty() {
                    self.store_offsets(partition, offset_i64).map_err(|err| MessageReceiverError {
                        kind: MessageReceiverErrorKind::SaveVersion,
                        version: Some(offset),
                        reason: err.to_string(),
                        data: Some(format!("{}", offset)),
                    })?;
                }

                ChannelMessage::Decision(offset, msg)
            }
        };

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

    async fn commit(&self) -> Result<(), SystemServiceError> {
        if self.tpl.count() > 0 {
            self.consumer
                .commit(&self.tpl, rdkafka::consumer::CommitMode::Async)
                .map_err(|err| MessageReceiverError {
                    kind: MessageReceiverErrorKind::CommitError,
                    version: None,
                    reason: err.to_string(),
                    data: None,
                })?;
        }
        Ok(())
    }
    async fn update_savepoint(&mut self, offset: i64) -> Result<(), SystemServiceError> {
        // let partition = self.tpl.;
        let tpl = self.tpl.elements_for_topic(&self.topic);
        if !tpl.is_empty() {
            let first = tpl.first().unwrap();
            let partition = first.partition();

            self.store_offsets(partition, offset).map_err(|err| MessageReceiverError {
                kind: MessageReceiverErrorKind::SaveVersion,
                version: None,
                reason: err.to_string(),
                data: None,
            })?;
        }
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
