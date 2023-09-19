use futures_util::future::try_join_all;

use crate::{
    core::MessengerSystemService,
    errors::{MessengerServiceError, MessengerServiceErrorKind, MessengerServiceResult},
};

// pub async fn talos_messenger_service() -> Result<((), ()), MessengerServiceError> {
//     // 0. Create required items.
//     //  a. Create Kafka consumer
//     let mut kafka_config = KafkaConfig::from_env(None);
//     kafka_config.group_id = env_var!("TALOS_MESSENGER_KAFKA_GROUP_ID");
//     kafka_config.extend(
//         None,
//         Some(
//             [
//                 ("enable.auto.commit".to_string(), "false".to_string()),
//                 ("auto.offset.reset".to_string(), "earliest".to_string()),
//                 // ("fetch.wait.max.ms".to_string(), "600".to_string()),
//                 // ("socket.keepalive.enable".to_string(), "true".to_string()),
//                 // ("acks".to_string(), "0".to_string()),
//             ]
//             .into(),
//         ),
//     );
//     let kafka_consumer = KafkaConsumer::new(&kafka_config);

//     // b. Subscribe to topic.
//     kafka_consumer.subscribe().await.unwrap();

//     let (tx_feedback_channel, mut rx_feedback_channel) = mpsc::channel(10_000);
//     let (tx_actions_channel, mut rx_actions_channel) = mpsc::channel(10_000);

//     let suffix_config = SuffixConfig {
//         capacity: 400_000,
//         prune_start_threshold: Some(2_000),
//         min_size_after_prune: None,
//     };
//     let suffix: Suffix<MessengerCandidate> = Suffix::with_config(suffix_config);

//     let mut inbound_service = MessengerInboundService {
//         message_receiver_abcast: kafka_consumer,
//         tx_actions_channel,
//         rx_feedback_channel,
//         suffix,
//     };

//     let inbound_service_handle = tokio::spawn(inbound_service.run());

//     let producer = KafkaProducer::new(&kafka_config);

//     let mut publish_service = PublishActionService {
//         publisher: TestMessengerPublisher,
//         rx_actions_channel,
//         tx_feedback_channel,
//     };
//     let publish_service_handle = tokio::spawn(publish_service.run());
//     try_join!(flatten_service_result(inbound_service_handle), flatten_service_result(publish_service_handle))
// }

pub struct TalosMessengerService {
    // pub system: System,
    pub services: Vec<Box<dyn MessengerSystemService + Send + Sync>>,
}

impl TalosMessengerService {
    pub async fn run(self) -> MessengerServiceResult {
        let service_handle = self.services.into_iter().map(|mut service| tokio::spawn(async move { service.run().await }));

        let k = try_join_all(service_handle).await.map_err(|e| MessengerServiceError {
            kind: MessengerServiceErrorKind::System,
            reason: e.to_string(),
            data: None,
            service: "Main thread".to_string(),
        })?;

        for res in k {
            res?
        }

        Ok(())
    }
}
