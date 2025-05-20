use ahash::HashMap;
use async_trait::async_trait;
use opentelemetry::global;
use rdkafka::producer::ProducerContext;
use talos_certifier::ports::{errors::MessagePublishError, MessageReciever};
use talos_certifier_adapters::KafkaConsumer;
use talos_common_utils::otel::initialiser::{init_otel_logs_tracing, init_otel_metrics};
use talos_messenger_core::{
    core::{MessengerPublisher, PublishActionType},
    errors::{MessengerServiceError, MessengerServiceErrorKind, MessengerServiceResult},
    services::{MessengerInboundService, MessengerInboundServiceConfig},
    suffix::MessengerCandidate,
    talos_messenger_service::TalosMessengerService,
};
use talos_rdkafka_utils::kafka_config::KafkaConfig;
use talos_suffix::{
    core::{SuffixConfig, SuffixMetricsConfig},
    Suffix,
};
use tokio::sync::mpsc;
use tracing::debug;

use crate::kafka::{
    context::{MessengerProducerContext, MessengerProducerDeliveryOpaque},
    models::KafkaAction,
    producer::KafkaProducer,
    service::KafkaActionService,
};

pub struct MessengerKafkaPublisher<C: ProducerContext + 'static> {
    pub publisher: KafkaProducer<C>,
}

#[async_trait]
impl<C> MessengerPublisher for MessengerKafkaPublisher<C>
where
    C: ProducerContext<DeliveryOpaque = Box<MessengerProducerDeliveryOpaque>> + 'static,
{
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
        additional_data: Self::AdditionalData,
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

        let delivery_opaque = MessengerProducerDeliveryOpaque {
            version,
            total_publish_count: additional_data,
        };

        let mut headers_to_publish = headers;

        if let Some(payload_header) = payload.headers {
            headers_to_publish.extend(payload_header);
        }

        self.publisher.publish_to_topic(
            &payload.topic,
            payload.partition,
            payload.key.as_deref(),
            payload_str,
            headers_to_publish,
            Box::new(delivery_opaque),
        )
    }
}

pub struct ChannelBuffers {
    pub actions_channel: u32,
    pub feedback_channel: u32,
}

impl Default for ChannelBuffers {
    fn default() -> Self {
        Self {
            actions_channel: 10_000,
            feedback_channel: 10_000,
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
    pub otel_grpc_endpoint: Option<String>,
}

pub async fn messenger_with_kafka(config: Configuration) -> MessengerServiceResult {
    init_otel_logs_tracing("messenger".into(), false, config.otel_grpc_endpoint.clone(), "info").map_err(|e| MessengerServiceError {
        kind: MessengerServiceErrorKind::System,
        reason: e.reason,
        data: e.cause,
        service: "init_otel_logs_tracing".into(),
    })?;

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

    let (tx_feedback_channel, rx_feedback_channel) = mpsc::channel(feedback_channel as usize);
    let (tx_actions_channel, rx_actions_channel) = mpsc::channel(actions_channel as usize);
    let tx_feedback_channel_clone = tx_feedback_channel.clone();

    // START - Inbound service
    let suffix: Suffix<MessengerCandidate> = if config.otel_grpc_endpoint.is_some() {
        let _ = init_otel_metrics(config.otel_grpc_endpoint.clone());
        let meter = global::meter("messenger");
        Suffix::with_config(
            config.suffix_config.unwrap_or_default(),
            Some((SuffixMetricsConfig { prefix: "messenger".into() }, meter)),
        )
    } else {
        Suffix::with_config(config.suffix_config.unwrap_or_default(), None)
    };

    let inbound_service_config = MessengerInboundServiceConfig::new(config.allowed_actions, config.commit_size, config.commit_frequency);

    let inbound_service = MessengerInboundService::new(kafka_consumer, tx_actions_channel, rx_feedback_channel, suffix, inbound_service_config);
    // END - Inbound service

    // START - Publish service
    let custom_context = MessengerProducerContext {
        tx_feedback_channel: tx_feedback_channel_clone,
    };
    let kafka_producer = KafkaProducer::with_context(&config.kafka_config, custom_context);
    let messenger_kafka_publisher = MessengerKafkaPublisher { publisher: kafka_producer };

    let publish_service = KafkaActionService {
        publisher: messenger_kafka_publisher.into(),
        rx_actions_channel,
        tx_feedback_channel,
    };

    // END - Publish service
    let messenger_service = TalosMessengerService {
        services: vec![Box::new(inbound_service), Box::new(publish_service)],
    };

    messenger_service.run().await
}
