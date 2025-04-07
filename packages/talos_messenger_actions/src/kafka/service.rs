use std::sync::Arc;

use async_trait::async_trait;
use futures_util::future::join_all;
use log::{debug, error, info};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::sync::mpsc;

use talos_messenger_core::{
    core::{ActionService, MessengerChannelFeedback, MessengerCommitActions, MessengerPublisher, MessengerSystemService},
    errors::MessengerServiceResult,
    suffix::MessengerStateTransitionTimestamps,
    utlis::get_action_deserialised,
};

use super::models::KafkaAction;

#[derive(Debug)]
pub struct KafkaActionService<M: MessengerPublisher<Payload = KafkaAction> + Send + Sync + 'static> {
    pub publisher: Arc<M>,
    pub rx_actions_channel: mpsc::Receiver<MessengerCommitActions>,
    pub tx_feedback_channel: mpsc::Sender<MessengerChannelFeedback>,
}

#[async_trait]
impl<M> ActionService for KafkaActionService<M>
where
    M: MessengerPublisher<Payload = KafkaAction, AdditionalData = u32> + Send + Sync,
{
    async fn process_action(&mut self) -> MessengerServiceResult {
        let actions_result = self.rx_actions_channel.recv().await;

        match actions_result {
            Some(actions) => {
                let MessengerCommitActions {
                    version,
                    commit_actions,
                    headers,
                } = actions;

                if let Some(publish_actions) = commit_actions.get(&self.publisher.get_publish_type().to_string()) {
                    let total_len = publish_actions.len() as u32;
                    let mut publish_vec = vec![];
                    for publish_action in publish_actions {
                        match get_action_deserialised::<KafkaAction>(publish_action.clone()) {
                            Ok(action) => {
                                let headers_cloned = headers.clone();

                                let publisher = self.publisher.clone();
                                let mut headers = headers_cloned.clone();
                                let timestamp = OffsetDateTime::now_utc().format(&Rfc3339).ok().unwrap();

                                headers.insert(MessengerStateTransitionTimestamps::EndOnCommitActions.to_string(), timestamp);
                                publish_vec.push(async move {
                                    if let Err(publish_error) = publisher.send(version, action, headers, total_len).await {
                                        error!("Failed to publish message for version={version} with error {publish_error}")
                                    }
                                });
                            }
                            Err(err) => {
                                error!(
                                    "Failed to deserialise for version={version} key={} for data={:?} with error={:?}",
                                    &self.publisher.get_publish_type(),
                                    err.data,
                                    err.reason
                                );
                            }
                        }
                    }
                    join_all(publish_vec).await;
                }
            }
            None => {
                debug!("No actions to process..")
            }
        }
        Ok(())
    }
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
            self.process_action().await?;
        }
    }

    async fn stop(&self) -> MessengerServiceResult {
        todo!()
    }
}
