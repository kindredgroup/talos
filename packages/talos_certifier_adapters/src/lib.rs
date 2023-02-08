// Kafka exports
pub mod kafka;
pub use kafka::config::Config as KakfaConfig;
pub use kafka::consumer::KafkaConsumer;
pub use kafka::errors::KafkaAdapterError;
pub use kafka::producer::KafkaProducer;

// postgres exports
pub mod postgres;
pub use postgres::config::PgConfig;
pub use postgres::errors::PgError;
pub use postgres::pg::Pg;

// custom certifiers with adapters
mod certifier_kafka_pg;
pub use certifier_kafka_pg::{certifier_with_kafka_pg, TalosCertifierChannelBuffers};
