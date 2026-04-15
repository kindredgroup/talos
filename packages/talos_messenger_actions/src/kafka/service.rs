use std::sync::Arc;

use async_trait::async_trait;
use futures_util::future::join_all;
use log::{debug, error, info};
use opentelemetry::{
    metrics::{Counter, Meter},
    KeyValue,
};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::sync::mpsc;

use talos_messenger_core::{
    core::{ActionService, MessengerChannelFeedback, MessengerCommitActions, MessengerPublisher, MessengerSystemService},
    errors::MessengerServiceResult,
    suffix::MessengerStateTransitionTimestamps,
    utlis::get_actions_deserialised,
};

use super::models::KafkaAction;

pub const METRIC_ERROR_TYPES: &str = "error_type";

#[derive(Debug)]
pub struct KafkaActionServiceOtelMetrics {
    /// Metric counter on the actions errors
    otel_actions_error_counters: Counter<u64>,
}

impl KafkaActionServiceOtelMetrics {
    pub(crate) fn new(meter: &Meter) -> Self {
        Self {
            otel_actions_error_counters: meter.u64_counter("talos_messenger_errors".to_string()).build(),
        }
    }
}

#[derive(Debug)]
pub struct KafkaActionService<M: MessengerPublisher<Payload = KafkaAction> + Send + Sync + 'static> {
    pub publisher: Arc<M>,
    pub rx_actions_channel: mpsc::Receiver<MessengerCommitActions>,
    pub tx_feedback_channel: mpsc::Sender<MessengerChannelFeedback>,
    pub otel_metrics: KafkaActionServiceOtelMetrics,
}

impl<M> KafkaActionService<M>
where
    M: MessengerPublisher<Payload = KafkaAction, AdditionalData = u32> + Send + Sync,
{
    pub fn new(
        publisher: Arc<M>,
        rx_actions_channel: mpsc::Receiver<MessengerCommitActions>,
        tx_feedback_channel: mpsc::Sender<MessengerChannelFeedback>,
        otel_meter: &Meter,
    ) -> Self {
        let otel_metrics = KafkaActionServiceOtelMetrics::new(otel_meter);
        Self {
            publisher,
            rx_actions_channel,
            tx_feedback_channel,
            otel_metrics,
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

        match actions_result {
            Some(actions) => {
                let MessengerCommitActions {
                    version,
                    commit_actions,
                    headers,
                } = actions;

                if let Some(publish_actions_for_type) = commit_actions.get(&self.publisher.get_publish_type().to_string()) {
                    match get_actions_deserialised::<Vec<KafkaAction>>(publish_actions_for_type) {
                        Ok(actions) => {
                            let total_len = actions.len() as u32;

                            let headers_cloned = headers.clone();

                            let publish_vec = actions.into_iter().map(|action| {
                                let publisher = self.publisher.clone();
                                let mut headers = headers_cloned.clone();
                                let timestamp = OffsetDateTime::now_utc().format(&Rfc3339).ok().unwrap();

                                headers.insert(MessengerStateTransitionTimestamps::EndOnCommitActions.to_string(), timestamp);
                                let otel_actions_error_counters = self.otel_metrics.otel_actions_error_counters.clone();
                                async move {
                                    if let Err(publish_error) = publisher.send(version, action, headers, total_len).await {
                                        error!("Failed to publish message for version={version} with error {publish_error}");
                                        otel_actions_error_counters.add(1, &[KeyValue::new(METRIC_ERROR_TYPES, "publish_message")]);
                                    }
                                }
                            });
                            join_all(publish_vec).await;
                        }
                        Err(err) => {
                            error!(
                                "Failed to deserialise for version={version} key={} for data={:?} with error={:?}",
                                &self.publisher.get_publish_type(),
                                err.data,
                                err.reason
                            );
                            self.otel_metrics
                                .otel_actions_error_counters
                                .add(1, &[KeyValue::new(METRIC_ERROR_TYPES, "deserialise_message")]);
                        }
                    }
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
