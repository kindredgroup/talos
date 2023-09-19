use async_trait::async_trait;
use log::info;
use tokio::sync::mpsc;

use crate::{
    core::{CommitActionType, MessengerChannelFeedback, MessengerCommitActions, MessengerPublisher, MessengerSystemService},
    errors::MessengerServiceResult,
    models::commit_actions::publish::KafkaAction,
    utlis::{get_sub_actions, get_value_by_key},
};

pub struct PublishActionService<M: MessengerPublisher<Payload = KafkaAction> + Send + Sync> {
    pub publisher: M,
    pub rx_actions_channel: mpsc::Receiver<MessengerCommitActions>,
    pub tx_feedback_channel: mpsc::Sender<MessengerChannelFeedback>,
}

#[async_trait]
impl<M> MessengerSystemService for PublishActionService<M>
where
    M: MessengerPublisher<Payload = KafkaAction> + Send + Sync,
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

                    let Some(publish_action) = get_value_by_key(&commit_actions, &CommitActionType::Publish.to_string()) else {
                        // If publish is not present, continue the loop.
                        continue;
                    };

                    // TODO: GK - Make this block generic in next ticket to iterator in loop by PublishActionType
                    {
                        let Some(kafka_actions) = get_sub_actions::<Vec<KafkaAction>>(&version, publish_action, &self.publisher.get_publish_type().to_string()) else {
                            continue;
                        };

                        for k_action in kafka_actions {
                            info!("Received message for version={version} and publish_action={k_action:#?}");
                            // if kafka commit action send to Kafka publisher
                            self.publisher.send(k_action).await;

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
