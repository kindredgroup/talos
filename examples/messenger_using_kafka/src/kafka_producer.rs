use async_trait::async_trait;
use log::info;
use rdkafka::producer::ProducerContext;
use talos_messenger::{
    core::{MessengerPublisher, PublishActionType},
    kafka::producer::{KafkaProducer, MessengerProducerDeliveryOpaque},
    models::commit_actions::publish::KafkaAction,
};

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

    async fn send(&self, version: u64, payload: Self::Payload, additional_data: Self::AdditionalData) -> () {
        info!("[MessengerKafkaPublisher] Publishing message with payload=\n{payload:#?}");

        let mut bytes: Vec<u8> = Vec::new();
        serde_json::to_writer(&mut bytes, &payload.value).unwrap();

        let payld = std::str::from_utf8(&bytes).unwrap();
        info!("[MessengerKafkaPublisher] base_record=\n{payld:#?}");

        let delivery_opaque = MessengerProducerDeliveryOpaque {
            version,
            total_publish_count: additional_data,
        };

        self.publisher
            .publish_to_topic("test.messenger.topic", "test", payld, None, Box::new(delivery_opaque))
            .unwrap();
    }
}
