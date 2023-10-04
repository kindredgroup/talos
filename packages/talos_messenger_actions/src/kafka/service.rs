use async_trait::async_trait;
use log::{error, info};
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

                    if let Some(publish_actions_for_type) = commit_actions.get(&self.publisher.get_publish_type().to_string()){
                        match  get_actions_deserialised::<Vec<KafkaAction>>(publish_actions_for_type) {
                            Ok(actions) => {

                                let total_len = actions.len() as u32;
                                for action in actions {
                                    // Publish the message
                                    self.publisher.send(version, action, total_len ).await;

                                }
                            },
                            Err(err) => {
                                error!("Failed to deserialise for version={version} key={} for data={:?} with error={:?}",&self.publisher.get_publish_type(), err.data, err.reason )
                            },
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
