use std::{marker::PhantomData, time::Duration};

use ahash::HashMap;
use async_trait::async_trait;
use futures_executor::block_on;
use log::{debug, info};
use rdkafka::{
    config::{FromClientConfig, FromClientConfigAndContext},
    error::KafkaError,
    producer::{BaseRecord, DefaultProducerContext, FutureProducer, FutureRecord, ProducerContext, ThreadedProducer},
    types::RDKafkaErrorCode,
    util::Timeout,
    IntoOpaque, Message,
};
use talos_certifier::{
    errors::SystemServiceError,
    ports::{common::SharedPortTraits, errors::MessagePublishError, MessagePublisher},
};
use talos_certifier_adapters::kafka::utils::build_kafka_headers;
use talos_rdkafka_utils::kafka_config::KafkaConfig;

// Kafka Producer
pub struct KafkaProducer<C, D>
where
    C: ProducerContext + 'static,
    D: IntoOpaque + Clone,
{
    producer: FutureProducer<DefaultProducerContext>,
    topic: String,
    _phantom: PhantomData<(D, C)>,
}

// impl KafkaProducer {
//     pub fn new(config: &KafkaConfig) -> Self {
//         let client_config = config.build_producer_config();
//         let producer = ThreadedProducer::from_config(&client_config).expect("Failed to create producer");
//         let topic = config.topic.to_owned();

//         Self {
//             producer,
//             topic,
//             _phantom: PhantomData,
//         }
//     }
// }
impl<C, D> KafkaProducer<C, D>
where
    C: ProducerContext<DeliveryOpaque = D> + 'static,
    D: IntoOpaque + Clone,
{
    pub fn with_context(config: &KafkaConfig, context: C) -> Self {
        let client_config = config.build_producer_config();
        let producer = FutureProducer::from_config_and_context(&client_config, DefaultProducerContext).expect("Failed to create producer");
        let topic = config.topic.to_owned();

        Self {
            producer,
            topic,
            _phantom: PhantomData,
        }
    }

    pub async fn publish_to_topic(
        &self,
        topic: &str,
        partition: Option<i32>,
        key: Option<&str>,
        value: &str,
        headers: HashMap<String, String>,
        delivery_opaque: D,
    ) -> Result<(), MessagePublishError> {
        let mut retry_count = 0;
        // TODO: GK - make the durations configurable.
        let min_duration_ms = Duration::from_millis(5);
        let max_duration_ms = Duration::from_millis(100);
        let mut result = Ok(());

        // let record = BaseRecord::with_opaque_to(topic, delivery_opaque.clone()).payload(value);

        // // Add partition if applicable
        // let record = if let Some(part) = partition { record.partition(part) } else { record };

        // // Add key if applicable
        // let record = if let Some(key_str) = key { record.key(key_str) } else { record };

        // // Add headers if applicable
        // let record = record.headers(build_kafka_headers(headers.clone()));

        loop {
            let new_record = FutureRecord::to(topic).payload(value);

            // Add partition if applicable
            let new_record = if let Some(part) = partition { new_record.partition(part) } else { new_record };

            // Add key if applicable
            let new_record = if let Some(key_str) = key { new_record.key(key_str) } else { new_record };

            // Add headers if applicable
            let new_record = new_record.headers(build_kafka_headers(headers.clone()));

            //TODO: GK - Remove the hardcoded timeout milliseconds.
            match self.producer.send(new_record, Timeout::After(Duration::from_millis(10))).await {
                Ok(_) => {
                    result = Ok(());
                    break;
                }

                Err((kafka_error, error_record)) => {
                    retry_count += 1;

                    let retry_duration = (min_duration_ms * retry_count).max(max_duration_ms);
                    info!(
                            "Failed to publish message for topic = {:?} | partition = {:?} | key = {:?}. Retrying attempt number {retry_count} in {} ms. Error {kafka_error:?}",
                            error_record.topic(),
                            error_record.partition(),
                            error_record.key(),
                            retry_duration.as_millis()
                        );
                    // tokio::time::sleep(min_duration_ms * retry_count).await;
                }
            };
        }

        result
        // self.producer.send(record).map_err(|(kafka_error, record)| MessagePublishError {
        //     reason: kafka_error.to_string(),
        //     data: Some(format!(
        //         "Topic={:?} partition={:?} key={:?} headers={:?} payload={:?}",
        //         self.topic, record.partition, record.key, record.headers, record.payload
        //     )),
        // })
    }
}

//  Message publisher traits
// #[async_trait]
// impl<C, D> MessagePublisher for KafkaProducer<C, D>
// where
//     C: ProducerContext<DeliveryOpaque = D> + 'static,
//     D: IntoOpaque + Clone,
// {
//     async fn publish_message(&self, key: &str, value: &str, headers: HashMap<String, String>) -> Result<(), SystemServiceError> {
//         let record = BaseRecord::to(&self.topic).payload(value).key(key);

//         let record = record.headers(build_kafka_headers(headers));

//         debug!("Preparing to publish the message. ");
//         let delivery_result = self
//             .producer
//             .send(record)
//             // .send(record, Timeout::After(Duration::from_secs(1)))
//             // .await
//             .map_err(|(kafka_error, record)| MessagePublishError {
//                 reason: kafka_error.to_string(),
//                 data: Some(format!("{:?}", record)),
//             })?;

//         debug!("Published the message successfully {:?} ", delivery_result.to_owned());
//         Ok(())
//     }
// }

#[async_trait]
impl<C, D> SharedPortTraits for KafkaProducer<C, D>
where
    C: ProducerContext + 'static,
    D: IntoOpaque + Clone,
{
    async fn is_healthy(&self) -> bool {
        true
    }
    async fn shutdown(&self) -> bool {
        true
    }
}
