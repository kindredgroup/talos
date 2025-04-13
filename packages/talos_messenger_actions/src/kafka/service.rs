use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use futures_util::future::join_all;
use log::{debug, error, info, warn};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::sync::{mpsc, Semaphore};

use talos_messenger_core::{
    core::{ActionService, MessengerChannelFeedback, MessengerCommitActions, MessengerPublisher, MessengerSystemService},
    errors::{MessengerActionError, MessengerServiceError, MessengerServiceErrorKind, MessengerServiceResult},
    suffix::MessengerStateTransitionTimestamps,
    utlis::get_action_deserialised,
};

use super::models::KafkaAction;

/// KafkaActionService configs.
#[derive(Debug)]
pub struct KafkaActionServiceConfig {
    /// Max time to wait before sending feedback when the feedback channel is at max capacity.
    /// - **Default** - 10ms
    pub max_feedback_await_send_ms: u32,
    /// Worker pool size
    /// - **Default** - 100_000. This is ideal for a workload running at 3K tps with each version having 5 `on_commit` actions (1:5 ratio of candidate message to actions).
    pub kafka_producer_worker_pool: u32,
}

impl KafkaActionServiceConfig {
    pub fn new(max_feedback_await_send_ms: Option<u32>, kafka_producer_worker_pool: Option<u32>) -> Self {
        Self {
            max_feedback_await_send_ms: max_feedback_await_send_ms.unwrap_or(10),
            kafka_producer_worker_pool: kafka_producer_worker_pool.unwrap_or(100_000),
        }
    }
}

#[derive(Debug)]
pub struct KafkaActionService<M: MessengerPublisher<Payload = KafkaAction> + Send + Sync + 'static> {
    pub publisher: Arc<M>,
    pub rx_actions_channel: mpsc::Receiver<MessengerCommitActions>,
    pub tx_feedback_channel: mpsc::Sender<MessengerChannelFeedback>,
    /// Limit the number of i/o tasks
    pub semaphore: Arc<Semaphore>,
    pub config: KafkaActionServiceConfig,
}

impl<M> KafkaActionService<M>
where
    M: MessengerPublisher<Payload = KafkaAction, AdditionalData = u32> + Send + Sync,
{
    pub fn new(
        publisher: Arc<M>,
        rx_actions_channel: mpsc::Receiver<MessengerCommitActions>,
        tx_feedback_channel: mpsc::Sender<MessengerChannelFeedback>,
        config: KafkaActionServiceConfig,
    ) -> Self {
        Self {
            publisher,
            rx_actions_channel,
            tx_feedback_channel,
            semaphore: Arc::new(Semaphore::new(config.kafka_producer_worker_pool as usize)),
            config,
        }
    }
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
                    let semaphore = self.semaphore.clone();
                    // Ensure we have enough permits to process all the `on_commit` actions under a version.
                    // This could indirectly cause back-pressure as the channel is full, and the other thread cannot push new messages into the channel
                    let permits_many = semaphore
                        .acquire_many_owned(publish_actions.len() as u32)
                        .await
                        .map_err(|_| MessengerServiceError {
                            kind: MessengerServiceErrorKind::Permits,
                            reason: "Error acquiring lock for kafka producer task. Permit semaphore may have closed already".to_string(),
                            data: None,
                            service: "Action service".to_string(),
                        })?;
                    tokio::spawn({
                        let total_len = publish_actions.len() as u32;
                        let publish_actions = publish_actions.clone();
                        let mut publish_vec = vec![];
                        let publish_action_type = self.publisher.get_publish_type().to_string();
                        let publisher = self.publisher.clone();
                        let feedback_channel = self.tx_feedback_channel.clone();
                        let max_feedback_await_send_ms = self.config.max_feedback_await_send_ms as u64;

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
                                            let feedback_message = if let Err(publish_error) = publisher.send(version, action.clone(), headers, total_len).await
                                            {
                                                error!("Failed to publish message for version={version} with error {publish_error}");

                                                let messenger_error = MessengerActionError {
                                                    kind: talos_messenger_core::errors::MessengerActionErrorKind::Publishing,
                                                    reason: publish_error.to_string(),
                                                    data: format!("version={version} message={action:?}"),
                                                };

                                                MessengerChannelFeedback::Error(version, publish_action_type, messenger_error.into())
                                            } else {
                                                MessengerChannelFeedback::Success(version, publish_action_type)
                                            };

                                            // Feedbacks shouldn't be dropped, therefore we retry till feedbacks eventually goes through.
                                            loop {
                                                match feedback_channel.try_send(feedback_message.clone()) {
                                                    Ok(_) => {
                                                        break;
                                                    }
                                                    Err(mpsc::error::TrySendError::Full(msg)) => {
                                                        warn!("Failed to send feedback over feedback_channel as channel is full. Retrying in {max_feedback_await_send_ms}ms. Feedback = {msg:?}");
                                                    }
                                                    Err(mpsc::error::TrySendError::Closed(msg)) => {
                                                        error!("Failed to send feedback over feedback_channel as channel is closed. Feedback = {msg:?}");
                                                    }
                                                };
                                                tokio::time::sleep(Duration::from_millis(max_feedback_await_send_ms)).await;
                                            }
                                        });
                                    }
                                    Err(err) => {
                                        error!(
                                            "Failed to deserialise for version={version} key={} for data={:?} with error={:?}",
                                            &publish_action_type, err.data, err.reason
                                        );
                                        let feedback_message = MessengerChannelFeedback::Error(version, publish_action_type, err.into());

                                        // Feedbacks shouldn't be dropped, therefore we retry till feedbacks eventually goes through.
                                        loop {
                                            match feedback_channel.try_send(feedback_message.clone()) {
                                                Ok(_) => {
                                                    break;
                                                }
                                                Err(mpsc::error::TrySendError::Full(msg)) => {
                                                    warn!("Failed to send feedback over feedback_channel as channel is full. Feedback = {msg:?}");
                                                }
                                                Err(mpsc::error::TrySendError::Closed(msg)) => {
                                                    warn!("Failed to send feedback over feedback_channel as channel is closed. Feedback = {msg:?}");
                                                }
                                            };
                                            tokio::time::sleep(Duration::from_millis(max_feedback_await_send_ms)).await;
                                        }
                                    }
                                }
                            }
                            join_all(publish_vec).await;
                            drop(permits_many);
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
