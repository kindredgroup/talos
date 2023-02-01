/// The implementation of publisher which communicates with kafka brokers.
use futures::executor;
use rdkafka::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use crate::api::{KafkaConfig};
use crate::messaging::api::{CandidateMessage, Publisher, PublishResponse};

pub struct KafkaPublisher {
    certification_topic: String,
    producer: FutureProducer,
}

impl KafkaPublisher {
    pub fn new(config: &KafkaConfig) -> KafkaPublisher {
        Self {
            certification_topic: config.certification_topic.clone(),
            producer: Self::create_producer(&config)
        }
    }

    fn create_producer(kafka: &KafkaConfig) -> FutureProducer {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", &kafka.brokers)
            .set("message.timeout.ms", &kafka.message_timeout_ms.to_string());

        return cfg.create().expect("Unable to create kafka producer");
    }
}
impl Publisher for KafkaPublisher {
    fn send_message(&self, key: String, message: CandidateMessage) -> Result<PublishResponse, String> {
        println!("Kafka: publishing message {:?} with key: {}", message, key);

        let h_type = Header { key: "messageType", value: Some("Candidate") };
        let payload = serde_json::to_string(&message).unwrap();

        let data = FutureRecord::to(self.certification_topic.as_str())
            .key(&key)
            .payload(&payload)
            .headers(OwnedHeaders::new().insert(h_type));

        return match self.producer.send_result(data) {
            Ok(df) => {
                let answer = executor::block_on(df);
                match answer {
                    Ok(v) => {
                        match v {
                            Ok((partition, offset)) => {
                                println!("Published into partition {}, offset: {}", partition, offset);
                                Ok(PublishResponse { partition, offset })
                            },
                            Err((e, _)) => {
                                println!("Publish response error2: {}", e);
                                Err(e.to_string())
                            }
                        }
                    },
                    Err(e) => {
                        println!("Publishing has been cancelled: {}", e);
                        Err(e.to_string())
                    }
                }
            },
            Err((kafka_error, _)) => {
                println!("Error publishing record: {}", kafka_error);
                Err(kafka_error.to_string())
            }
        }
    }
}
