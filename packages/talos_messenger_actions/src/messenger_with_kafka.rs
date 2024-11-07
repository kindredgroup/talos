use ahash::HashMap;
use async_trait::async_trait;
use log::{debug, warn};
use rdkafka::producer::ProducerContext;
use talos_certifier::ports::MessageReciever;
use talos_certifier_adapters::KafkaConsumer;
use talos_messenger_core::{
    core::{MessengerPublisher, PublishActionType},
    errors::MessengerServiceResult,
    services::MessengerInboundService,
    suffix::MessengerCandidate,
    talos_messenger_service::TalosMessengerService,
};
use talos_rdkafka_utils::kafka_config::KafkaConfig;
use talos_suffix::{core::SuffixConfig, Suffix};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::sync::mpsc;

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

    async fn send(&self, version: u64, payload: Self::Payload, headers: HashMap<String, String>, additional_data: Self::AdditionalData) -> () {
        debug!("[MessengerKafkaPublisher] Publishing message with payload=\n{payload:#?}");

        let mut bytes: Vec<u8> = Vec::new();
        serde_json::to_writer(&mut bytes, &payload.value).unwrap();

        let payload_str = std::str::from_utf8(&bytes).unwrap();
        debug!("[MessengerKafkaPublisher] base_record=\n{payload_str:#?}");

        let delivery_opaque = MessengerProducerDeliveryOpaque {
            version,
            total_publish_count: additional_data,
        };

        let h1 = headers.get("decisionCreatedTimestamp").map(|x| x.clone());

        let mut headers_to_publish = headers;

        if let Some(payload_header) = payload.headers {
            headers_to_publish.extend(payload_header);
        }

        self.publisher
            .publish_to_topic(
                &payload.topic,
                payload.partition,
                payload.key.as_deref(),
                payload_str,
                headers_to_publish,
                Box::new(delivery_opaque),
            )
            .unwrap();

        // if let Some(t1) = h1 {
        //     let ts1 = OffsetDateTime::parse(&t1, &Rfc3339).unwrap();
        //     let timestamp = OffsetDateTime::now_utc().format(&Rfc3339).ok().unwrap();
        //     let ts2 = OffsetDateTime::now_utc();
        //     let diff_time = ts2 - ts1;
        //     warn!(
        //         "Time taken from decision created to on_commit action publish = {:?}ms | decision_create_ts = {t1} | on_commit_publish_ts = {timestamp}",
        //         diff_time.as_seconds_f64() / 1_000_f64
        //     );
        // }
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
}

pub async fn messenger_with_kafka(config: Configuration) -> MessengerServiceResult {
    let kafka_consumer = KafkaConsumer::new(&config.kafka_config);

    // Subscribe to topic.
    kafka_consumer.subscribe().await.unwrap();

    let ChannelBuffers {
        actions_channel,
        feedback_channel,
    } = config.channel_buffers.unwrap_or_default();

    let (tx_feedback_channel, rx_feedback_channel) = mpsc::channel(feedback_channel as usize);
    let (tx_actions_channel, rx_actions_channel) = mpsc::channel(actions_channel as usize);
    let tx_feedback_channel_clone = tx_feedback_channel.clone();

    // START - Inbound service
    let suffix: Suffix<MessengerCandidate> = Suffix::with_config(config.suffix_config.unwrap_or_default());

    let inbound_service = MessengerInboundService::new(kafka_consumer, tx_actions_channel, rx_feedback_channel, suffix, config.allowed_actions);
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
