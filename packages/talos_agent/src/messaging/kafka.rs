use crate::api::KafkaConfig;
use crate::messaging::api::{CandidateMessage, PublishResponse, Publisher};
use async_trait::async_trait;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use std::time::Duration;

/// The implementation of publisher which communicates with kafka brokers.

pub struct KafkaPublisher {
    config: KafkaConfig,
    producer: FutureProducer,
}

impl KafkaPublisher {
    pub fn new(config: &KafkaConfig) -> KafkaPublisher {
        Self {
            config: config.clone(),
            producer: Self::create_producer(config),
        }
    }

    fn create_producer(kafka: &KafkaConfig) -> FutureProducer {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", &kafka.brokers)
            .set("message.timeout.ms", &kafka.message_timeout_ms.to_string());

        cfg.create().expect("Unable to create kafka producer")
    }
}

#[async_trait]
impl Publisher for KafkaPublisher {
    async fn send_message(&self, key: String, message: CandidateMessage) -> Result<PublishResponse, String> {
        println!("Kafka: async publishing message {:?} with key: {}", message, key);

        let h_type = Header {
            key: "messageType",
            value: Some("Candidate"),
        };
        let payload = serde_json::to_string(&message).unwrap();

        let data = FutureRecord::to(self.config.certification_topic.as_str())
            .key(&key)
            .payload(&payload)
            .headers(OwnedHeaders::new().insert(h_type));

        let timeout = Timeout::After(Duration::from_millis(self.config.enqueue_timout_ms));
        return match self.producer.send(data, timeout).await {
            Ok((partition, offset)) => {
                println!("Published into partition {}, offset: {}", partition, offset);
                Ok(PublishResponse { partition, offset })
            }
            Err((e, _)) => {
                println!("Publishing error: {}", e);
                Err(e.to_string())
            }
        };
    }
}
