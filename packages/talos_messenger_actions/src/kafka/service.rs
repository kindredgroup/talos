use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use futures_util::future::join_all;
use log::{debug, error, info, warn};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::sync::mpsc;

use talos_messenger_core::{
    core::{ActionService, MessengerChannelFeedback, MessengerCommitActions, MessengerPublisher, MessengerSystemService},
    errors::{MessengerActionError, MessengerServiceResult},
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
        // if self.rx_actions_channel.max_capacity() - self.rx_actions_channel.capacity() > 0 {
        //     warn!(
        //         "Number of actions on channel = {:?}",
        //         self.rx_actions_channel.max_capacity() - self.rx_actions_channel.capacity()
        //     );
        // }

        match actions_result {
            Some(actions) => {
                let MessengerCommitActions {
                    version,
                    commit_actions,
                    headers,
                } = actions;

                if let Some(publish_actions) = commit_actions.get(&self.publisher.get_publish_type().to_string()) {
                    tokio::spawn({
                        let total_len = publish_actions.len() as u32;
                        let publish_actions = publish_actions.clone();
                        let mut publish_vec = vec![];
                        let publish_action_type = self.publisher.get_publish_type().to_string();
                        let publisher = self.publisher.clone();
                        let feedback_channel = self.tx_feedback_channel.clone();
                        async move {
                            for publish_action in publish_actions {
                                let publish_action_type = publish_action_type.clone();
                                let publisher = publisher.clone();
                                let feedback_channel = feedback_channel.clone();

                                match get_action_deserialised::<KafkaAction>(publish_action.clone()) {
                                    Ok(action) => {
                                        let headers_cloned = headers.clone();

                                        let mut headers = headers_cloned.clone();
                                        let timestamp = OffsetDateTime::now_utc().format(&Rfc3339).ok().unwrap();

                                        headers.insert(MessengerStateTransitionTimestamps::EndOnCommitActions.to_string(), timestamp);
                                        let feedback_channel = feedback_channel.clone();
                                        publish_vec.push(async move {
                                            if let Err(publish_error) = publisher.send(version, action.clone(), headers, total_len).await {
                                                error!("Failed to publish message for version={version} with error {publish_error}");

                                                loop {
                                                    let messenger_error = MessengerActionError {
                                                        kind: talos_messenger_core::errors::MessengerActionErrorKind::Publishing,
                                                        reason: publish_error.to_string(),
                                                        data: format!("version={version} message={action:?}"),
                                                    };
                                                    match feedback_channel
                                                        .send_timeout(
                                                            MessengerChannelFeedback::Error(version, "kafka".to_string(), messenger_error.into()),
                                                            Duration::from_millis(40), // TODO: GK - Avoid hardcoded values.
                                                        )
                                                        .await
                                                    {
                                                        Ok(_) => break,
                                                        Err(_) => {}
                                                    };
                                                }

                                                // if let Err(send_feedback_error) = feedback_channel
                                                //     .send(MessengerChannelFeedback::Error(version, publish_action_type, messenger_error.into()))
                                                //     .await
                                                // {
                                                //     error!("Failed sending feedback over channel due to error {send_feedback_error:?}");
                                                // };
                                            } else {
                                                loop {
                                                    match feedback_channel
                                                        .send_timeout(
                                                            MessengerChannelFeedback::Success(version, "kafka".to_string()),
                                                            Duration::from_millis(40), // TODO: GK - Avoid hardcoded values.
                                                        )
                                                        .await
                                                    {
                                                        Ok(_) => break,
                                                        Err(_) => {}
                                                    };
                                                }
                                            }
                                        });
                                    }
                                    Err(err) => {
                                        error!(
                                            "Failed to deserialise for version={version} key={} for data={:?} with error={:?}",
                                            &publish_action_type, err.data, err.reason
                                        );
                                        if let Err(send_feedback_error) = feedback_channel
                                            .send(MessengerChannelFeedback::Error(version, publish_action_type, err.into()))
                                            .await
                                        {
                                            error!("Failed sending feedback over channel due to error {send_feedback_error:?}");
                                        };
                                    }
                                }
                            }
                            join_all(publish_vec).await;
                        }
                    });
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
