use std::{marker::PhantomData, sync::Arc, time::Duration};

use async_trait::async_trait;
use log::{debug, warn};
use rdkafka::{
    consumer::{Consumer, ConsumerContext, DefaultConsumerContext, StreamConsumer},
    Message, TopicPartitionList,
};
use talos_certifier::{
    core::{CandidateChannelMessage, CandidateMessageBaseTrait, DecisionChannelMessage, MessageVariant},
    errors::SystemServiceError,
    model::DecisionMessage,
    ports::{
        common::SharedPortTraits,
        errors::{MessageReceiverError, MessageReceiverErrorKind},
        MessageReciever,
    },
    ChannelMessage,
};
use talos_common_utils::backpressure::controller::BackPressureTimeout;
use talos_rdkafka_utils::kafka_config::KafkaConfig;
use time::OffsetDateTime;
use tokio::task::JoinHandle;

use crate::{kafka::utils::get_message_headers, KafkaAdapterError};

use super::utils;

// Kafka Consumer Client
// #[derive(Debug, Clone)]
pub struct KafkaConsumer<M, C = DefaultConsumerContext>
where
    C: ConsumerContext,
{
    pub consumer: Arc<StreamConsumer<C>>,
    pub topic: String,
    pub tpl: TopicPartitionList,
    /// If consumer is paused for tpl
    is_paused: bool,
    _phantom: PhantomData<M>,
}

impl<M> KafkaConsumer<M, DefaultConsumerContext> {
    /// Create a new Kafka consumer with `DefaultConsumerContext`
    pub fn new(config: &KafkaConfig) -> Self {
        KafkaConsumer::with_context(config, DefaultConsumerContext)
    }
}

impl<M, C> KafkaConsumer<M, C>
where
    C: ConsumerContext + 'static,
{
    /// Create a new Kafka consumer with custom consumer context.
    pub fn with_context(config: &KafkaConfig, context: C) -> Self {
        let consumer = config.build_consumer_config().create_with_context(context).expect("Failed to create consumer");

        let topic = config.topic.clone();
        Self {
            consumer: Arc::new(consumer),
            topic,
            tpl: TopicPartitionList::new(),
            is_paused: false,
            _phantom: PhantomData,
        }
    }

    pub fn store_offsets(&mut self, partition: i32, offset: i64) -> Result<(), KafkaAdapterError> {
        debug!("Partition {partition} and Offset {offset}");

        let offset_to_update = offset + 1;

        match self.tpl.find_partition(&self.topic, partition) {
            Some(tpl) => {
                let offset_in_tpl = tpl.offset().to_raw().unwrap_or_default();

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
impl<M, C> MessageReciever for KafkaConsumer<M, C>
where
    M: CandidateMessageBaseTrait + Send + Sync,
    C: ConsumerContext + 'static,
{
    type Message = ChannelMessage<M>;

    async fn consume_message_with_backpressure(&mut self, timeout: BackPressureTimeout) -> Result<Option<Self::Message>, MessageReceiverError> {
        match timeout {
            BackPressureTimeout::Timeout(time_ms) => {
                if self.is_paused {
                    tracing::warn!("[kafka consumer timeout] Resuming consumption of new message");
                    self.consumer.resume(&self.tpl).map_err(|e| MessageReceiverError {
                        kind: MessageReceiverErrorKind::PauseResume,
                        version: None,
                        reason: e.to_string(),
                        data: Some(self.topic.clone()),
                    })?;
                    self.is_paused = false;
                }
                if time_ms > 0 {
                    tracing::warn!("[kafka consumer timeout] Sleeping for {time_ms}ms, before consuming the message");
                    tokio::time::sleep(Duration::from_millis(time_ms)).await;
                }
                self.consume_message().await
            }
            BackPressureTimeout::CompleteStop(max_time_ms) => {
                if !self.is_paused {
                    tracing::warn!("[kafka consumer timeout] Pausing consumption of new message due to being");
                    self.consumer.pause(&self.tpl).map_err(|e| MessageReceiverError {
                        kind: MessageReceiverErrorKind::PauseResume,
                        version: None,
                        reason: e.to_string(),
                        data: Some(self.topic.clone()),
                    })?;
                    self.is_paused = true;
                    tokio::time::sleep(Duration::from_millis(max_time_ms)).await;
                }
                Ok(None)
            }
            // BackPressureTimeout::MaxTimeout(max_time_ms, count) => {
            //     if !self.is_paused && count > 1 {
            //         tracing::warn!("[kafka consumer timeout] Pausing consumption of new message due to being on constant max timeout");
            //         self.consumer.pause(&self.tpl).map_err(|e| MessageReceiverError {
            //             kind: MessageReceiverErrorKind::PauseResume,
            //             version: None,
            //             reason: e.to_string(),
            //             data: Some(self.topic.clone()),
            //         })?;
            //         self.is_paused = true;

            //         tokio::time::sleep(Duration::from_millis(max_time_ms)).await;
            //     }
            //     Ok(None)
            // }
            BackPressureTimeout::NoTimeout => {
                if self.is_paused {
                    tracing::warn!("[kafka consumer timeout] Resuming consumption of new message");
                    self.consumer.resume(&self.tpl).map_err(|e| MessageReceiverError {
                        kind: MessageReceiverErrorKind::PauseResume,
                        version: None,
                        reason: e.to_string(),
                        data: Some(self.topic.clone()),
                    })?;
                    self.is_paused = false;
                }
                self.consume_message().await
            }
        }
    }

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
                let mut msg: M = utils::parse_kafka_payload(raw_payload).map_err(|e| MessageReceiverError {
                    kind: MessageReceiverErrorKind::ParseError,
                    version: Some(offset),
                    reason: e.to_string(),
                    data: Some(format!("{:?}", String::from_utf8_lossy(raw_payload))),
                })?;
                msg.add_version(offset);
                msg.add_candidate_received_metric(OffsetDateTime::now_utc().unix_timestamp_nanos());
                ChannelMessage::Candidate(
                    CandidateChannelMessage {
                        message: msg,
                        headers: headers.clone(),
                    }
                    .into(),
                )
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

                ChannelMessage::Decision(
                    DecisionChannelMessage {
                        decision_version: offset,
                        message: msg,
                        headers: headers.clone(),
                    }
                    .into(),
                )
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

    fn commit(&self) -> Result<(), Box<SystemServiceError>> {
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

    fn commit_async(&self) -> Option<JoinHandle<Result<(), SystemServiceError>>> {
        if self.tpl.count() > 0 {
            let consumer_copy = Arc::clone(&self.consumer);
            let tpl = self.tpl.clone();
            let handle = tokio::task::spawn(async move {
                consumer_copy.commit(&tpl, rdkafka::consumer::CommitMode::Async).map_err(|err| {
                    MessageReceiverError {
                        kind: MessageReceiverErrorKind::CommitError,
                        version: None,
                        reason: err.to_string(),
                        data: None,
                    }
                    .into()
                })
            });

            Some(handle)
        } else {
            None
        }
    }

    fn update_offset_to_commit(&mut self, offset: i64) -> Result<(), Box<SystemServiceError>> {
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

    async fn update_savepoint_async(&mut self, _offset: i64) -> Result<(), SystemServiceError> {
        // For future, maybe for another abcast. Not needed in here.
        unimplemented!()
    }
}

#[async_trait]
impl<M, C> SharedPortTraits for KafkaConsumer<M, C>
where
    M: Send + Sync,
    C: ConsumerContext,
{
    async fn is_healthy(&self) -> bool {
        matches!(self.consumer.client().fetch_metadata(None, Duration::from_secs(2)), Ok(_result))
    }
    async fn shutdown(&self) -> bool {
        self.consumer.unsubscribe();
        true
    }
}
