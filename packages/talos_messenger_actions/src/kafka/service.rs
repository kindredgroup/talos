use async_trait::async_trait;
use log::info;
use tokio::sync::mpsc;

use talos_messenger_core::{
    core::{MessengerChannelFeedback, MessengerCommitActions, MessengerPublisher, MessengerSystemService},
    errors::MessengerServiceResult,
    utlis::get_actions_deserialised,
};

use super::models::KafkaAction;

pub struct KafkaActionService<M: MessengerPublisher<Payload = KafkaAction> + Send + Sync> {
    pub publisher: M,
    pub rx_actions_channel: mpsc::Receiver<MessengerCommitActions>,
    pub tx_feedback_channel: mpsc::Sender<MessengerChannelFeedback>,
}

#[async_trait]
impl<M> MessengerSystemService for KafkaActionService<M>
where
    M: MessengerPublisher<Payload = KafkaAction, AdditionalData = u32> + Send + Sync,
{
    async fn start(&self) -> MessengerServiceResult {
        todo!()
    }
    async fn run(&mut self) -> MessengerServiceResult {
        info!("Running Kafka Publisher service!!");
        loop {
            tokio::select! {
                Some(actions) = self.rx_actions_channel.recv() => {
                    let MessengerCommitActions {version, commit_actions } = actions;

                    let Some(publish_actions_for_type) = commit_actions.get(&self.publisher.get_publish_type().to_string()) else {
                        // If publish is not present, continue the loop.
                        continue;
                    };

                    // TODO: GK - Make this block generic in next ticket to iterator in loop by PublishActionType
                    {
                        let Some(kafka_actions) = get_actions_deserialised::<Vec<KafkaAction>>(&version, publish_actions_for_type, &self.publisher.get_publish_type().to_string()) else {
                            continue;
                        };
                        let total_len = kafka_actions.len() as u32;
                        for k_action in kafka_actions {
                            info!("Received message for version={version} and publish_action={k_action:#?}");


                            // if kafka commit action send to Kafka publisher
                            self.publisher.send(version, k_action, total_len ).await;

                        }
                    }
                }
            }
        }
    }

    async fn stop(&self) -> MessengerServiceResult {
        todo!()
    }
}
