use crate::messaging::api::{CandidateMessage, PublishResponse, Publisher};
use async_trait::async_trait;

/// The mock publisher
pub struct MockPublisher;

#[async_trait]
impl Publisher for MockPublisher {
    async fn send_message(&self, key: String, message: CandidateMessage) -> Result<PublishResponse, String> {
        println!("Mock: async publishing message {:?} with key: {}", message, key);
        Ok(PublishResponse { partition: 0, offset: 0 })
    }
}
