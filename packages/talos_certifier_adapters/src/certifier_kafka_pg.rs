use crate::kafka::contexts::CertifierConsumerContext;
use crate::mock_certifier_service::MockCertifierService;
use crate::PgConfig;
use crate::{self as Adapters, KafkaConsumer};
use std::sync::{atomic::AtomicI64, Arc};
use talos_certifier::core::SystemService;
use talos_certifier::model::{CandidateMessage, DecisionMessage};
use talos_certifier::ports::DecisionStore;

use talos_certifier::services::{CertifierServiceConfig, MessageReceiverServiceConfig};
use talos_certifier::{
    core::{DecisionOutboxChannelMessage, System},
    errors::SystemServiceError,
    services::{CertifierService, DecisionOutboxService, MessageReceiverService},
    talos_certifier_service::{TalosCertifierService, TalosCertifierServiceBuilder},
};
use talos_rdkafka_utils::kafka_config::KafkaConfig;
use talos_suffix::core::SuffixConfig;
use tokio::sync::{broadcast, mpsc};

/// Channel buffer values
pub struct TalosCertifierChannelBuffers {
    system_broadcast: usize,
    message_receiver: usize,
    decision_outbox: usize,
}

impl Default for TalosCertifierChannelBuffers {
    fn default() -> Self {
        Self {
            system_broadcast: 3_000,
            message_receiver: 30_000,
            decision_outbox: 30_000,
        }
    }
}

pub struct Configuration {
    pub suffix_config: Option<SuffixConfig>,
    pub pg_config: Option<PgConfig>,
    pub kafka_config: KafkaConfig,
    pub certifier_mock: bool,
    pub db_mock: bool,
    /// Minimum offsets to retain.
    /// Higher the value, lesser the frequency of commit.
    /// This would help in hydrating the suffix in certifier so as to help suffix retain a minimum size to
    /// reduce the initial aborts for fresh certification requests.
    /// - **Default: 50_000**
    pub min_commit_threshold: Option<u64>,
    /// Frequency at which to commit should be issued in ms
    /// - **Default: 10_000** - 10 seconds
    pub commit_frequency_ms: Option<u64>,
    /// If not set, defaults to `talos_certifier`.
    /// Setting this can be useful to distinguish unique deployed version of talos certifier for easier debugging.
    pub app_name: Option<String>,
}

/// Talos certifier instantiated with Kafka as Abcast and Postgres as XDB.
pub async fn certifier_with_kafka_pg(channel_buffer: TalosCertifierChannelBuffers, config: Configuration) -> Result<TalosCertifierService, SystemServiceError> {
    let (system_notifier, _) = broadcast::channel(channel_buffer.system_broadcast);

    let system = System {
        system_notifier,
        is_shutdown: false,
        name: config.app_name.unwrap_or("talos_certifier".to_string()),
    };

    let (tx, rx) = mpsc::channel(channel_buffer.message_receiver);

    /* START - Kafka consumer service  */
    let commit_offset: Arc<AtomicI64> = Arc::new(0.into());

    let kafka_consumer: KafkaConsumer<CandidateMessage, CertifierConsumerContext> = Adapters::KafkaConsumer::with_context(
        &config.kafka_config,
        CertifierConsumerContext {
            topic: config.kafka_config.topic.clone(),
            message_channel_tx: tx.clone(),
        },
    );
    // let kafka_consumer_service = KafkaConsumerService::new(kafka_consumer, tx, system.clone());
    let message_receiver_service = MessageReceiverService::new(
        Box::new(kafka_consumer),
        tx,
        Arc::clone(&commit_offset),
        system.clone(),
        MessageReceiverServiceConfig {
            commit_frequency_ms: config.commit_frequency_ms.unwrap_or(10_000),
            min_commit_threshold: config.min_commit_threshold.unwrap_or(50_000),
        },
    );

    message_receiver_service.subscribe().await?;
    /* END - Kafka consumer service  */

    let (outbound_tx, outbound_rx) = mpsc::channel::<DecisionOutboxChannelMessage>(channel_buffer.decision_outbox);

    /* START - Certifier service  */
    let certifier_service: Box<dyn SystemService + Send + Sync> = match config.certifier_mock {
        true => Box::new(MockCertifierService {
            decision_outbox_tx: outbound_tx.clone(),
            message_channel_rx: rx,
        }),
        false => Box::new(CertifierService::new(
            rx,
            outbound_tx.clone(),
            Arc::clone(&commit_offset),
            system.clone(),
            Some(CertifierServiceConfig {
                suffix_config: config.suffix_config.unwrap_or_default(),
            }),
        )),
    };

    /* END - Certifier service  */

    /* START - Decision Outbox service  */

    let kafka_producer = Adapters::KafkaProducer::new(&config.kafka_config);
    let data_store: Box<dyn DecisionStore<Decision = DecisionMessage> + Send + Sync> = match config.db_mock {
        true => Box::new(Adapters::mock_datastore::MockDataStore {}),
        false => Box::new(Adapters::Pg::new(config.pg_config.expect("pg config is required without mock.")).await.unwrap()),
    };
    let decision_outbox_service = DecisionOutboxService::new(
        outbound_tx.clone(),
        outbound_rx,
        Arc::new(data_store),
        Arc::new(Box::new(kafka_producer)),
        system.clone(),
    );
    /* END - Decision Outbox service  */

    let talos_certifier = TalosCertifierServiceBuilder::new(system)
        .add_certifier_service(certifier_service)
        .add_adapter_service(Box::new(message_receiver_service))
        .add_adapter_service(Box::new(decision_outbox_service))
        .build();

    Ok(talos_certifier)
}
