use std::sync::{atomic::AtomicI64, Arc};

use crate as Adapters;
use talos_certifier::{
    core::{DecisionOutboxChannelMessage, System},
    errors::SystemServiceError,
    services::{CertifierService, DecisionOutboxService, MessageReceiverService},
    talos_certifier_service::{TalosCertifierService, TalosCertifierServiceBuilder},
};

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

/// Talos certifier instantiated with Kafka as Abcast and Postgres as XDB.
pub async fn certifier_with_kafka_pg(channel_buffer: TalosCertifierChannelBuffers) -> Result<TalosCertifierService, SystemServiceError> {
    let (system_notifier, _) = broadcast::channel(channel_buffer.system_broadcast);

    let system = System {
        system_notifier,
        is_shutdown: false,
    };

    let (tx, rx) = mpsc::channel(channel_buffer.message_receiver);
    let commit_offset: Arc<AtomicI64> = Arc::new(0.into());

    /* START - Kafka consumer service  */
    let kafka_config = Adapters::KakfaConfig::from_env();

    let kafka_consumer = Adapters::KafkaConsumer::new(&kafka_config);
    // let kafka_consumer_service = KafkaConsumerService::new(kafka_consumer, tx, system.clone());
    let message_receiver_service = MessageReceiverService::new(Box::new(kafka_consumer), tx, Arc::clone(&commit_offset), system.clone());

    message_receiver_service.subscribe().await?;
    /* END - Kafka consumer service  */

    let (outbound_tx, outbound_rx) = mpsc::channel::<DecisionOutboxChannelMessage>(channel_buffer.decision_outbox);

    /* START - Certifier service  */

    let certifier_service = CertifierService::new(rx, outbound_tx.clone(), Arc::clone(&commit_offset), system.clone());
    /* END - Certifier service  */

    /* START - Decision Outbox service  */
    let pg_config = Adapters::PgConfig::from_env();
    let pg = Adapters::Pg::new(pg_config.clone()).await.unwrap();
    let kafka_producer = Adapters::KafkaProducer::new(&kafka_config);

    let decision_outbox_service = DecisionOutboxService::new(
        outbound_tx.clone(),
        outbound_rx,
        Arc::new(Box::new(pg)),
        Arc::new(Box::new(kafka_producer)),
        system.clone(),
    );
    /* END - Decision Outbox service  */

    let talos_certifier = TalosCertifierServiceBuilder::new(system)
        .add_certifier_service(Box::new(certifier_service))
        .add_adapter_service(Box::new(message_receiver_service))
        .add_adapter_service(Box::new(decision_outbox_service))
        .build();

    Ok(talos_certifier)
}
