use crate as Adapters;
use crate::kafka::config::KafkaConfig;
use crate::mock_certifier_service::MockCertifierService;
use crate::PgConfig;
use std::sync::{atomic::AtomicI64, Arc};
use talos_certifier::core::SystemService;
use talos_certifier::model::DecisionMessage;
use talos_certifier::ports::DecisionStore;

use talos_certifier::services::CertifierServiceConfig;
use talos_certifier::{
    core::{DecisionOutboxChannelMessage, System},
    errors::SystemServiceError,
    services::{CertifierService, DecisionOutboxService, MessageReceiverService},
    talos_certifier_service::{TalosCertifierService, TalosCertifierServiceBuilder},
};
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

pub struct Configuration<'a> {
    pub suffix_config: Option<SuffixConfig>,
    pub pg_config: Option<PgConfig>,
    pub kafka_config: KafkaConfig<'a>,
    pub certifier_mock: bool,
    pub db_mock: bool,
}

/// Talos certifier instantiated with Kafka as Abcast and Postgres as XDB.
pub async fn certifier_with_kafka_pg<'a>(
    channel_buffer: TalosCertifierChannelBuffers,
    configuration: Configuration<'_>,
) -> Result<TalosCertifierService, SystemServiceError> {
    let (system_notifier, _) = broadcast::channel(channel_buffer.system_broadcast);

    let system = System {
        system_notifier,
        is_shutdown: false,
    };

    let (tx, rx) = mpsc::channel(channel_buffer.message_receiver);
    let commit_offset: Arc<AtomicI64> = Arc::new(0.into());

    let kafka_consumer = Adapters::KafkaConsumer::new(&configuration.kafka_config);
    // let kafka_consumer_service = KafkaConsumerService::new(kafka_consumer, tx, system.clone());
    let message_receiver_service = MessageReceiverService::new(Box::new(kafka_consumer), tx, Arc::clone(&commit_offset), system.clone());

    message_receiver_service.subscribe().await?;
    /* END - Kafka consumer service  */

    let (outbound_tx, outbound_rx) = mpsc::channel::<DecisionOutboxChannelMessage>(channel_buffer.decision_outbox);

    /* START - Certifier service  */
    let certifier_service: Box<dyn SystemService + Send + Sync> = match configuration.certifier_mock {
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
                suffix_config: configuration.suffix_config.unwrap_or_default(),
            }),
        )),
    };

    /* END - Certifier service  */

    /* START - Decision Outbox service  */

    let kafka_producer = Adapters::KafkaProducer::new(&configuration.kafka_config);
    let data_store: Box<dyn DecisionStore<Decision = DecisionMessage> + Send + Sync> = match configuration.db_mock {
        true => Box::new(Adapters::mock_datastore::MockDataStore {}),
        false => Box::new(
            Adapters::Pg::new(configuration.pg_config.expect("pg config is required without mock."))
                .await
                .unwrap(),
        ),
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
