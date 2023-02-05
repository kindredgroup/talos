use std::sync::{atomic::AtomicI64, Arc};

use log::{error, info};
use talos_adapters as Adapters;
use talos_core::{
    config::Config,
    core::{DecisionOutboxChannelMessage, System},
    errors::SystemServiceError,
    services::{CertifierService, DecisionOutboxService, HealthCheckService, MessageReceiverService},
    talos_certifier_service::TalosCertifierServiceBuilder,
};

use tokio::{
    signal,
    sync::{broadcast, mpsc},
};

use logger::logs;

#[tokio::main]
async fn main() -> Result<(), SystemServiceError> {
    logs::init();

    let (system_notifier, _) = broadcast::channel(3_000);

    let system = System {
        system_notifier,
        is_shutdown: false,
    };

    let config = Config::from_env();

    let (tx, rx) = mpsc::channel(30_000);
    let commit_offset: Arc<AtomicI64> = Arc::new(0.into());

    /* START - Health Check service  */

    let _healthcheck_service = HealthCheckService::new(system.clone());
    /* END - Health Check service  */

    /* START - Kafka consumer service  */
    let kafka_config = Adapters::KakfaConfig {
        brokers: config.kafka_brokers,
        topic_prefix: config.kafka_topic_prefix,
        consumer_topic: config.kafka_topic.clone(),
        producer_topic: config.kafka_topic,
        client_id: config.kafka_client_id,
        group_id: config.kafka_group_id,
        username: config.kafka_username,
        password: config.kafka_password,
    };

    let kafka_consumer = Adapters::KafkaConsumer::new(&kafka_config);
    // let kafka_consumer_service = KafkaConsumerService::new(kafka_consumer, tx, system.clone());
    let message_receiver_service = MessageReceiverService::new(Box::new(kafka_consumer), tx, Arc::clone(&commit_offset), system.clone());

    message_receiver_service.subscribe().await.unwrap();
    /* END - Kafka consumer service  */

    let (outbound_tx, outbound_rx) = mpsc::channel::<DecisionOutboxChannelMessage>(30_000);

    /* START - Certifier service  */

    let certifier_service = CertifierService::new(rx, outbound_tx.clone(), Arc::clone(&commit_offset), system.clone());
    /* END - Certifier service  */

    /* START - Decision Outbox service  */
    let pg_config = Adapters::PgConfig {
        user: config.pg_user,
        password: config.pg_password,
        host: config.pg_host,
        port: "5432".to_owned(),
        database: config.pg_database,
    };
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
        // .add_health_check_service(Box::new(healthcheck_service))
        .add_adapter_service(Box::new(message_receiver_service))
        .add_adapter_service(Box::new(decision_outbox_service))
        .build();

    // Services thread thread spawned
    let svc_handle = tokio::spawn(async move { talos_certifier.run().await });

    // Look for termination signals
    tokio::select! {
        // Talos certifier handle
        res = svc_handle => {
            if let Err(error) = res.unwrap() {
                error!("Talos Certifier shutting down due to error!!!");
                 return Err(error);
            }
        }
        // CTRL + C termination signal
        _ = signal::ctrl_c() => {
            info!("CTRL + C TERMINATION!!!!");
        },
    };

    info!("Talos Certifier shutdown!!!");

    Ok(())
}
