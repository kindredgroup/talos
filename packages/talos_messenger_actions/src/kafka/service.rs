use std::sync::Arc;

use ahash::HashMap;
use async_trait::async_trait;
use futures_util::future::join_all;
use log::{debug, error, info};
use opentelemetry::{
    metrics::{Counter, Histogram, Meter},
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

use crate::kafka::utils::{parse_header_timestamps, time_diff_micoseconds};

use super::models::KafkaAction;

pub const METRIC_ERROR_TYPES: &str = "error_type";

#[derive(Debug)]
pub struct KafkaActionServiceOtelMetrics {
    /// Metric counter on the actions errors
    otel_actions_error_counters: Counter<u64>,
    /// Metric histogram candidate received in messenger to publish on_commit actions
    otel_candidate_received_to_publish_action_histogram: Histogram<f64>,
    /// Metric histogram decision received in messenger to publish on_commit actions
    otel_decision_received_to_publish_action_histogram: Histogram<f64>,
    /// Metric histogram decision received in messenger to pick on_commit actions to be published
    /// A decision received doesn't automatically mean the actions will be published if there are messages
    /// prior to it with dependency waiting to be published.
    otel_decision_received_to_pick_action_histogram: Histogram<f64>,
    /// Metric histogram for on_commit action picked to when it was published.
    otel_action_picked_published_histogram: Histogram<f64>,
    /// Metric histogram for duration from initiator to action published.
    otel_initiator_publish_action_histogram: Histogram<f64>,
}

impl KafkaActionServiceOtelMetrics {
    pub(crate) fn new(meter: &Meter) -> Self {
        Self {
            otel_actions_error_counters: meter.u64_counter("talos_messenger_errors".to_string()).build(),
            otel_candidate_received_to_publish_action_histogram: meter
                .f64_histogram("talos_messenger_candidate_received_publish_action".to_string())
                .with_description("Duration in talos messenger from when the candidate was received to when the on_commit action was published")
                .with_unit("ms")
                .build(),
            otel_decision_received_to_publish_action_histogram: meter
                .f64_histogram("talos_messenger_decision_received_publish_action".to_string())
                .with_description("Duration in talos messenger from when the decision was received to when the on_commit action was published")
                .with_unit("us")
                .build(),
            otel_decision_received_to_pick_action_histogram: meter
                .f64_histogram("talos_messenger_decision_received_pick_action".to_string())
                .with_description("Duration in talos messenger from when the decision was received to when the on_commit action was picked")
                .with_unit("us")
                .build(),
            otel_action_picked_published_histogram: meter
                .f64_histogram("talos_messenger_action_picked_published".to_string())
                .with_description("Duration in talos messenger from action picked to process till published the on_commit action")
                .with_unit("us")
                .build(),
            otel_initiator_publish_action_histogram: meter
                .f64_histogram("talos_initiator_messenger_publish_action".to_string())
                .with_description("Duration from candidate created in initiator to talos messenger published the on_commit action")
                .with_unit("ms")
                .build(),
        }
    }

    pub(crate) fn record_metrics_from_kafka_header(&mut self, headers: &HashMap<String, String>) {
        let end_on_commit_time = OffsetDateTime::now_utc();

        let candidate_received_time = headers
            .get(&MessengerStateTransitionTimestamps::CandidateReceived.to_string())
            .and_then(|t| parse_header_timestamps(t));
        let decision_received_time = headers
            .get(&MessengerStateTransitionTimestamps::DecisionReceived.to_string())
            .and_then(|t| parse_header_timestamps(t));
        let start_on_commit_actions_time = headers
            .get(&MessengerStateTransitionTimestamps::StartOnCommitActions.to_string())
            .and_then(|t| parse_header_timestamps(t));
        let initiator_candidate_created_time = headers.get("cohortCandidateCreatedAtTimestamp").and_then(|t| parse_header_timestamps(t));
        // Publish the following metrics
        // 1. Candidate received to Messenger on_commit publish
        if let Some(candidate_received_time) = candidate_received_time {
            self.otel_candidate_received_to_publish_action_histogram
                .record(time_diff_micoseconds(candidate_received_time, end_on_commit_time) as f64 / 1_000_f64, &[]);
        }
        // 2. Decision received to Messenger on_commit publish
        if let Some(decision_received_time) = decision_received_time {
            self.otel_decision_received_to_publish_action_histogram
                .record(time_diff_micoseconds(decision_received_time, end_on_commit_time) as f64, &[]);
        }
        // 3. Decision received to Messenger picked on_commit messages
        if let (Some(decision_received_time), Some(on_commit_actions_picked_time)) = (decision_received_time, start_on_commit_actions_time) {
            self.otel_decision_received_to_pick_action_histogram
                .record(time_diff_micoseconds(decision_received_time, on_commit_actions_picked_time) as f64, &[]);
        }
        // 4. on_commit message picked to published - (Picked in Inbound Service and passed to Action Service from where it will be published)
        if let Some(start_on_commit_actions_time) = start_on_commit_actions_time {
            self.otel_action_picked_published_histogram
                .record(time_diff_micoseconds(start_on_commit_actions_time, end_on_commit_time) as f64, &[]);
        }
        // 5. Initiator to on_commit message published
        if let Some(initiator_candidate_created_time) = initiator_candidate_created_time {
            self.otel_initiator_publish_action_histogram.record(
                time_diff_micoseconds(initiator_candidate_created_time, end_on_commit_time) as f64 / 1_000_f64,
                &[],
            );
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
                                if let Ok(end_on_commit_timestamp_string) = OffsetDateTime::now_utc().format(&Rfc3339) {
                                    headers.insert(
                                        MessengerStateTransitionTimestamps::EndOnCommitActions.to_string(),
                                        end_on_commit_timestamp_string,
                                    );
                                }
                                // Record the otel metrics for durations at between various points
                                self.otel_metrics.record_metrics_from_kafka_header(&headers);

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

#[cfg(test)]
mod tests {
    use ahash::AHashMap;
    use ahash::HashMap;
    use async_trait::async_trait;
    use opentelemetry::metrics::Meter;
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::error::OTelSdkResult;
    use opentelemetry_sdk::metrics::data::ResourceMetrics;
    use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
    use opentelemetry_sdk::metrics::{SdkMeterProvider, Temporality};
    use std::sync::{Arc, Mutex};
    use talos_messenger_core::suffix::MessengerStateTransitionTimestamps;

    use crate::kafka::service::KafkaActionServiceOtelMetrics;

    #[derive(Debug, Clone, Default)]
    struct TestExporter {
        metrics: Arc<Mutex<Vec<String>>>, // store metric names
    }

    #[async_trait]
    impl PushMetricExporter for TestExporter {
        async fn export(&self, metrics: &mut ResourceMetrics) -> OTelSdkResult {
            for scope in &metrics.scope_metrics {
                for metric in &scope.metrics {
                    self.metrics.lock().unwrap().push(metric.name.to_string());
                }
            }
            Ok(())
        }

        async fn force_flush(&self) -> OTelSdkResult {
            Ok(())
        }

        fn shutdown(&self) -> OTelSdkResult {
            Ok(())
        }

        fn temporality(&self) -> Temporality {
            Temporality::Cumulative
        }
    }

    fn setup_meter() -> (Meter, TestExporter, SdkMeterProvider) {
        let exporter = TestExporter::default();
        let provider = SdkMeterProvider::builder().with_periodic_exporter(exporter.clone()).build();
        let meter = provider.meter("test");
        (meter, exporter, provider)
    }

    fn base_headers() -> HashMap<String, String> {
        HashMap::from_iter([
            (
                MessengerStateTransitionTimestamps::CandidateReceived.to_string(),
                "2026-04-19T20:53:11.000Z".to_string(),
            ),
            (
                MessengerStateTransitionTimestamps::DecisionReceived.to_string(),
                "2026-04-19T20:53:11.100Z".to_string(),
            ),
            (
                MessengerStateTransitionTimestamps::StartOnCommitActions.to_string(),
                "2026-04-19T20:53:11.200Z".to_string(),
            ),
            ("cohortCandidateCreatedAtTimestamp".to_string(), "2026-04-19T20:53:10.000Z".to_string()),
        ])
    }

    #[tokio::test]
    async fn test_metric_struct_all_headers_present() {
        let (meter, exporter, provider) = setup_meter();
        let mut metric = KafkaActionServiceOtelMetrics::new(&meter);

        metric.record_metrics_from_kafka_header(&base_headers());

        // Before flush there are no metrics available
        let recorded = exporter.metrics.lock().unwrap();
        assert_eq!(recorded.len(), 0);
        drop(recorded);

        // Flush the metrics recorded
        provider.force_flush().unwrap();

        // After flush, all the metrics are available
        let recorded = exporter.metrics.lock().unwrap();
        assert_eq!(recorded.len(), 5);
        assert!(recorded.contains(&"talos_messenger_candidate_received_publish_action".to_string()));
        assert!(recorded.contains(&"talos_messenger_decision_received_publish_action".to_string()));
        assert!(recorded.contains(&"talos_messenger_decision_received_pick_action".to_string()));
        assert!(recorded.contains(&"talos_messenger_action_picked_published".to_string()));
        assert!(recorded.contains(&"talos_initiator_messenger_publish_action".to_string()));
    }

    #[tokio::test]
    async fn test_empty_headers_records_nothing() {
        let (meter, exporter, provider) = setup_meter();
        let mut metric = KafkaActionServiceOtelMetrics::new(&meter);

        metric.record_metrics_from_kafka_header(&AHashMap::new());
        provider.force_flush().unwrap();

        let finished = exporter.metrics.lock().unwrap();
        assert!(finished.is_empty());
    }

    #[tokio::test]
    async fn test_missing_candidate_received_skips_histogram_1() {
        let (meter, exporter, provider) = setup_meter();
        let mut metric = KafkaActionServiceOtelMetrics::new(&meter);
        let mut headers = base_headers();
        headers.remove(&MessengerStateTransitionTimestamps::CandidateReceived.to_string());

        metric.record_metrics_from_kafka_header(&headers);
        provider.force_flush().unwrap();

        let recorded = exporter.metrics.lock().unwrap();
        assert!(!recorded.contains(&"talos_messenger_candidate_received_publish_action".to_string()));
    }
}
