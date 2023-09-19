use async_trait::async_trait;
use log::info;
use talos_certifier::ports::MessagePublisher;
use talos_certifier_adapters::KafkaProducer;
use talos_messenger::{
    core::{MessengerPublisher, PublishActionType},
    models::commit_actions::publish::KafkaAction,
};

pub struct MessengerKafkaPublisher {
    pub publisher: KafkaProducer,
}

#[async_trait]
impl MessengerPublisher for MessengerKafkaPublisher {
    type Payload = KafkaAction;
    fn get_publish_type(&self) -> PublishActionType {
        PublishActionType::Kafka
    }

    async fn send(&self, payload: Self::Payload) -> () {
        info!("[MessengerKafkaPublisher] Publishing message with payload=\n{payload:#?}");

        let mut bytes: Vec<u8> = Vec::new();
        serde_json::to_writer(&mut bytes, &payload.value).unwrap();

        let payld = std::str::from_utf8(&bytes).unwrap();
        info!("[MessengerKafkaPublisher] base_record=\n{payld:#?}");

        self.publisher.publish_message("test", payld, None).await.unwrap();
    }
}
