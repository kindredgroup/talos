use crate::messaging::api::{CandidateMessage, Consumer, DecisionMessage, PublishResponse, Publisher};
use async_trait::async_trait;

/// The mock publisher.
pub struct MockPublisher;

#[async_trait]
impl Publisher for MockPublisher {
    async fn send_message(&self, key: String, message: CandidateMessage) -> Result<PublishResponse, String> {
        println!("Mock: async publishing message {:?} with key: {}", message, key);
        Ok(PublishResponse { partition: 0, offset: 0 })
    }
}

/// The mock consumer.
pub struct MockConsumer;

#[async_trait]
impl Consumer for MockConsumer {
    async fn receive_message(&self) -> Option<Result<DecisionMessage, String>> {
        None
    }
}
