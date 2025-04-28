// Kafka exports
pub mod kafka;
pub use kafka::consumer::KafkaConsumer;
pub use kafka::errors::KafkaAdapterError;
pub use kafka::producer::KafkaProducer;

// postgres exports
pub mod postgres;
pub use postgres::config::PgConfig;
pub use postgres::errors::PgError;
pub use postgres::pg::Pg;

// mock implementations
mod mock_certifier_service;
mod mock_datastore;

// custom certifiers with adapters
mod certifier_kafka_pg;

// custom certifier context
pub mod certifier_context;

pub use certifier_kafka_pg::{certifier_with_kafka_pg, Configuration, TalosCertifierChannelBuffers};
