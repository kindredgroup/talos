use async_trait::async_trait;
use log::{info, warn};
use tokio::sync::mpsc;

use crate::{
    core::{MessengerChannelFeedback, MessengerCommitActions, MessengerPublisher, MessengerSystemService},
    errors::MessengerServiceResult,
    models::commit_actions::publish::{KafkaActions, OnCommitActions, PublishActions},
    utlis::{get_sub_actions, get_value_by_key},
};

pub struct PublishActionService<M: MessengerPublisher> {
    pub publisher: M,
    pub rx_actions_channel: mpsc::Receiver<MessengerCommitActions>,
    pub tx_feedback_channel: mpsc::Sender<MessengerChannelFeedback>,
}

#[async_trait]
impl<M> MessengerSystemService for PublishActionService<M>
where
    M: MessengerPublisher + Send + Sync,
{
    async fn start(&self) -> MessengerServiceResult {
        todo!()
    }
    async fn run(&mut self) -> MessengerServiceResult {
        info!("Running Publisher service");
        loop {
            tokio::select! {
                Some(actions) = self.rx_actions_channel.recv() => {
                    let MessengerCommitActions {version, commit_actions } = actions;
                    let Some(publish_action) = get_value_by_key(&commit_actions, "publish") else {

                        continue;
                    };

                    let Some(kafka_actions) = get_sub_actions::<Vec<KafkaActions>>(&10, publish_action, "kafka") else {
                        continue;
                    };

                    for k_action in kafka_actions {
                        info!("Received message for version={version} and publish_action={k_action:#?}");
                        // if kafka commit action send to Kafka publisher
                        self.publisher.send().await;

                    }
                }
            }
        }
    }

    async fn stop(&self) -> MessengerServiceResult {
        todo!()
    }
}
