use rdkafka::error::KafkaError;
use thiserror::Error as ThisError;

#[derive(Debug, Clone, Eq, PartialEq, ThisError)]
pub enum KafkaAdapterError {
    // Kafka specific errors
    #[error("Error in subscribing to topic")]
    SubscribeTopic(#[source] KafkaError, String),
    #[error("Error receiving message")]
    ReceiveMessage(#[source] KafkaError),
    #[error("Error committing offset {1:?}")]
    Commit(#[source] KafkaError, Option<i64>),
    #[error("Error publishing message {1}")]
    PublishMessage(#[source] KafkaError, String),
    #[error("Unhandled Kafka exception")]
    UnhandledKafkaException(#[from] KafkaError),

    // Other errors like validation or parsing
    #[error("Required header ({0}) not found")]
    HeaderNotFound(String),
    #[error("Unknown Message Type {0}")]
    UnknownMessageType(String),
    #[error("Error parsing received message {0}")]
    MessageParsing(String),
    #[error("Offset Zero message will be skipped")]
    OffsetZeroMessage,
    #[error("Empty Payload")]
    EmptyPayload,
    #[error("Unknown Exception")]
    UnknownException(String),
}
