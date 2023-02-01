/// The mock publisher
use crate::messaging::api::{CandidateMessage, Publisher, PublishResponse};

pub struct MockPublisher;

impl Publisher for MockPublisher {
    fn send_message(&self, key: String, msg: CandidateMessage) -> Result<PublishResponse, String> {
        println!("Mock: publishing message {:?} with key: {}", msg, key);
        Ok(PublishResponse { partition: 0, offset: 0 })
    }
}
