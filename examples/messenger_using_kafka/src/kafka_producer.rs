use ahash::HashMap;
use async_trait::async_trait;
use log::info;
use rdkafka::producer::ProducerContext;
use talos_messenger_actions::kafka::{context::MessengerProducerDeliveryOpaque, models::KafkaAction, producer::KafkaProducer};
use talos_messenger_core::core::{MessengerPublisher, PublishActionType};

pub struct MessengerKafkaPublisher<C: ProducerContext + 'static> {
    pub publisher: KafkaProducer<C>,
}

#[async_trait]
impl<C> MessengerPublisher for MessengerKafkaPublisher<C>
where
    C: ProducerContext<DeliveryOpaque = Box<MessengerProducerDeliveryOpaque>> + 'static,
{
    type Payload = KafkaAction;
    type AdditionalData = u32;
    fn get_publish_type(&self) -> PublishActionType {
        PublishActionType::Kafka
    }

    async fn send(&self, version: u64, payload: Self::Payload, headers: HashMap<String, String>, additional_data: Self::AdditionalData) -> () {
        info!("[MessengerKafkaPublisher] Publishing message with payload=\n{payload:#?}");

        let mut bytes: Vec<u8> = Vec::new();
        serde_json::to_writer(&mut bytes, &payload.value).unwrap();

        let payload_str = std::str::from_utf8(&bytes).unwrap();
        info!("[MessengerKafkaPublisher] base_record=\n{payload_str:#?}");

        let delivery_opaque = MessengerProducerDeliveryOpaque {
            version,
            total_publish_count: additional_data,
        };

        self.publisher
            .publish_to_topic(
                &payload.topic,
                payload.partition,
                payload.key.as_deref(),
                payload_str,
                headers,
                Box::new(delivery_opaque),
            )
            .unwrap();
    }
}
