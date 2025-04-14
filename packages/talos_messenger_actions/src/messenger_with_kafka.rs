use ahash::HashMap;
use async_trait::async_trait;
use log::{debug, error};
use talos_certifier::ports::{errors::MessagePublishError, MessageReciever};
use talos_certifier_adapters::KafkaConsumer;
use talos_messenger_core::{
    core::{MessengerPublisher, PublishActionType},
    errors::{MessengerServiceError, MessengerServiceErrorKind, MessengerServiceResult},
    services::{MessengerInboundService, MessengerInboundServiceConfig},
    suffix::MessengerCandidate,
    talos_messenger_service::TalosMessengerService,
};
use talos_rdkafka_utils::kafka_config::KafkaConfig;
use talos_suffix::{core::SuffixConfig, Suffix};
use tokio::sync::mpsc;

use crate::kafka::{
    models::KafkaAction,
    producer::KafkaProducer,
    service::{KafkaActionService, KafkaActionServiceConfig},
};

pub struct MessengerKafkaPublisher {
    pub publisher: KafkaProducer,
}

#[async_trait]
impl MessengerPublisher for MessengerKafkaPublisher {
    type Payload = KafkaAction;
    type AdditionalData = u32;
    fn get_publish_type(&self) -> PublishActionType {
        PublishActionType::Kafka
    }

    async fn send(
        &self,
        version: u64,
        payload: Self::Payload,
        headers: HashMap<String, String>,
        _additional_data: Self::AdditionalData,
    ) -> Result<(), MessagePublishError> {
        debug!("[MessengerKafkaPublisher] Publishing message with payload=\n{payload:#?}");

        let mut bytes: Vec<u8> = Vec::new();
        serde_json::to_writer(&mut bytes, &payload.value).map_err(|e| MessagePublishError {
            reason: format!("failed to serialize payload for version={version} with error {e}").to_string(),
            data: Some(payload.value.to_string()),
        })?;

        let payload_str = std::str::from_utf8(&bytes).map_err(|e| MessagePublishError {
            reason: format!("failed to parse payload for version={version} with error {e}").to_string(),
            data: Some(payload.value.to_string()),
        })?;

        debug!("[MessengerKafkaPublisher] base_record=\n{payload_str:#?}");

        let mut headers_to_publish = headers;

        if let Some(payload_header) = payload.headers {
            headers_to_publish.extend(payload_header);
        }

        self.publisher
            .publish_to_topic(&payload.topic, payload.partition, payload.key.as_deref(), payload_str, headers_to_publish)
            .await
    }
}

/// For 1 action message for a candidate, the could be `n` on_commit actions.
/// The defaults are set at 1:10 for a target rate of 2K tps. i.e for 1 candidate action message on actions channel, there could be around 10 actions,
/// and therefore 10 feedbacks.
/// - `actions_channel` default - 10_000
/// - `feedback_channel` default -100_000
pub struct ChannelBuffers {
    pub actions_channel: u32,
    pub feedback_channel: u32,
}

impl Default for ChannelBuffers {
    fn default() -> Self {
        Self {
            actions_channel: 10_000,
            feedback_channel: 100_000,
        }
    }
}

pub struct Configuration {
    /// Configuration required for the In memory suffix
    pub suffix_config: Option<SuffixConfig>,
    /// Configuration required for the Kafka producer and consumer
    pub kafka_config: KafkaConfig,
    /// Map of permitted on_commit actions
    pub allowed_actions: HashMap<String, Vec<String>>,
    /// Channel buffer size for the internal channels between threads
    pub channel_buffers: Option<ChannelBuffers>,
    /// Commit size to decide how often the certifier topic can be committed by the consumer.
    /// The more often the commit is done has inverse impact on the latency.
    /// Defaults to 5_000.
    pub commit_size: Option<u32>,
    /// Commit issuing frequency.
    pub commit_frequency: Option<u32>,
    /// Number of worker threads to spawn at a time, to control the number of kafka publishes can happen parallely.
    /// - Limits the amount of produces adding messages to the queue parallely. (Less chances of running into Queue Full scenario and impacting latency).
    /// - Also helps in controlling the amount of on_commit actions to be deserialized parallely. (Less CPU load).
    /// - Also controls the feedbacks coming in, thereby reducing the chances of the feedbacks channel being filled up quite often.
    /// - **Default** - 100_000. This is ideal for a workload running at 3K tps with each version having 5 `on_commit` actions (1:5 ratio of candidate message to actions).
    pub kafka_producer_worker_pool: Option<u32>,
    /// When the number of messages coming is very high and if the feedbacks are not being processed and the channel filling up,
    /// we need to introduce a small delay before reading messages so that the feedbacks can be picked up and processed.
    /// - **Default** - 5ms
    pub max_timeout_before_read_message_ms: Option<u32>,
    /// Max time to wait before sending feedback when the feedback channel is at max capacity.
    /// - **Default** - 10ms
    pub max_feedback_await_send_ms: Option<u32>,
}

pub async fn messenger_with_kafka(config: Configuration) -> MessengerServiceResult {
    let kafka_consumer = KafkaConsumer::new(&config.kafka_config);

    // Subscribe to topic.
    kafka_consumer.subscribe().await.map_err(|e| MessengerServiceError {
        kind: MessengerServiceErrorKind::Messaging,
        reason: e.reason,
        data: e.data,
        service: e.service,
    })?;

    let ChannelBuffers {
        actions_channel,
        feedback_channel,
    } = config.channel_buffers.unwrap_or_default();

    // let (tx_feedback_channel, rx_feedback_channel) = mpsc::channel(feedback_channel as usize);
    let (tx_feedback_channel, rx_feedback_channel) = mpsc::unbounded_channel();
    let (tx_actions_channel, rx_actions_channel) = mpsc::channel(actions_channel as usize);

    // START - Inbound service
    let suffix: Suffix<MessengerCandidate> = Suffix::with_config(config.suffix_config.unwrap_or_default());
    let inbound_service_config = MessengerInboundServiceConfig::new(
        config.allowed_actions,
        config.commit_size,
        config.commit_frequency,
        config.max_timeout_before_read_message_ms,
    );
    let inbound_service = MessengerInboundService::new(kafka_consumer, tx_actions_channel, rx_feedback_channel, suffix, inbound_service_config);
    // END - Inbound service

    // START - Publish service
    let kafka_producer = KafkaProducer::new(&config.kafka_config);
    let messenger_kafka_publisher = MessengerKafkaPublisher { publisher: kafka_producer };

    let kafka_action_service_config = KafkaActionServiceConfig::new(config.max_feedback_await_send_ms, config.kafka_producer_worker_pool);

    let publish_service = KafkaActionService::new(
        messenger_kafka_publisher.into(),
        rx_actions_channel,
        tx_feedback_channel,
        kafka_action_service_config,
    );

    // END - Publish service
    let messenger_service = TalosMessengerService {
        services: vec![Box::new(inbound_service), Box::new(publish_service)],
    };

    if let Err(err) = messenger_service.run().await {
        error!("Exiting messenger service due to error {err:?}");
        Err(err)
    } else {
        Ok(())
    }
}
