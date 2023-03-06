use rdkafka::error::KafkaError;
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
#[error("Error with messaging system.\nReason: {reason}")]
pub struct MessagingError {
    pub reason: String,
}

impl From<KafkaError> for MessagingError {
    fn from(e: KafkaError) -> Self {
        MessagingError {
            reason: format!("Kafka error.\nReason: {}", e),
        }
    }
}
