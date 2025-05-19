// $coverage:ignore-start
use std::{fmt::Debug, time::Duration};

use crate::{
    core::{Replicator, ReplicatorChannel},
    errors::ReplicatorError,
    models::{ReplicatorCandidate, ReplicatorCandidateMessage},
    suffix::ReplicatorSuffixTrait,
    StatemapQueueChannelMessage,
};

use opentelemetry::{global, metrics::Gauge, trace::TraceContextExt, KeyValue};

use talos_certifier::{model::DecisionMessageTrait, ports::MessageReciever, ChannelMessage};
use talos_common_utils::otel::{
    metric_constants::{
        METRIC_KEY_CERT_DECISION_TYPE, METRIC_KEY_CERT_MESSAGE_TYPE, METRIC_NAME_CERTIFICATION_OFFSET, METRIC_VALUE_CERT_DECISION_TYPE_UNKNOWN,
        METRIC_VALUE_CERT_MESSAGE_TYPE_CANDIDATE, METRIC_VALUE_CERT_MESSAGE_TYPE_DECISION,
    },
    propagated_context::PropagatedSpanContextData,
};

use time::OffsetDateTime;
use tokio::{sync::mpsc, time::Interval};
use tracing::Instrument;
use tracing::{debug, error, info};
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub struct ReplicatorServiceConfig {
    /// Frequency in milliseconds
    pub commit_frequency_ms: u64,
    /// Enable internal stats
    pub enable_stats: bool,
}

struct ReplicatorMetrics {
    /// Records the number of items in the statemap channel.
    g_statemaps_tx: Gauge<u64>,
    /// Records the versions received from message receiver.
    g_consumed_offset: Gauge<u64>,
}

impl ReplicatorMetrics {
    pub fn new() -> Self {
        let g_statemaps_tx: opentelemetry::metrics::Gauge<u64> = global::meter("sdk_replicator").u64_gauge("repl_statemaps_channel").with_unit("items").build();
        let g_consumed_offset = global::meter("sdk_replicator")
            .u64_gauge(format!("repl_{}", METRIC_NAME_CERTIFICATION_OFFSET))
            .build();

        Self {
            g_statemaps_tx,
            g_consumed_offset,
        }
    }

    pub fn record_consumed_offset(&self, offset: u64, attributes: &[KeyValue]) {
        self.g_consumed_offset.record(offset, attributes);
    }

    pub fn record_statemap_tx_count(&self, count: u64) {
        self.g_statemaps_tx.record(count, &[]);
    }
}

pub struct ReplicatorService<S, M>
where
    S: ReplicatorSuffixTrait<ReplicatorCandidate> + Debug,
    M: MessageReciever<Message = ChannelMessage<ReplicatorCandidateMessage>> + Send + Sync,
{
    statemaps_tx: mpsc::Sender<StatemapQueueChannelMessage>,
    replicator_rx: mpsc::Receiver<ReplicatorChannel>,
    pub replicator: Replicator<ReplicatorCandidate, S, M>,
    #[allow(dead_code)]
    config: ReplicatorServiceConfig,
    _interval: Interval,
    metrics: ReplicatorMetrics,
}

impl<S, M> ReplicatorService<S, M>
where
    S: ReplicatorSuffixTrait<ReplicatorCandidate> + Debug,
    M: MessageReciever<Message = ChannelMessage<ReplicatorCandidateMessage>> + Send + Sync,
{
    pub fn new(
        statemaps_tx: mpsc::Sender<StatemapQueueChannelMessage>,
        replicator_rx: mpsc::Receiver<ReplicatorChannel>,
        replicator: Replicator<ReplicatorCandidate, S, M>,
        config: ReplicatorServiceConfig,
    ) -> Self {
        let metrics = ReplicatorMetrics::new();
        let interval = tokio::time::interval(Duration::from_millis(config.commit_frequency_ms));
        Self {
            statemaps_tx,
            replicator_rx,
            replicator,
            config,
            metrics,
            _interval: interval,
        }
    }

    pub async fn run_once(&mut self) -> Result<(), ReplicatorError> {
        tokio::select! {
            // 1. Consume message.
            res = self.replicator.receiver.consume_message() => {
                if let Ok(Some(msg)) = res {
                    // 2. Add/update to suffix.
                    match msg {
                        // 2.1 For CM - Install messages on the version
                        ChannelMessage::Candidate(candidate) => {
                            let offset = candidate.message.version;
                            info!(
                                "[Version>>{offset}] candidate recieved in replicator service {:?}ms",
                                OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000_i128
                            );
                            self.replicator.process_consumer_message(offset, candidate.message.into()).await;

                            self.metrics.record_consumed_offset(offset, &[
                                KeyValue::new(METRIC_KEY_CERT_MESSAGE_TYPE, METRIC_VALUE_CERT_MESSAGE_TYPE_CANDIDATE),
                                KeyValue::new(METRIC_KEY_CERT_DECISION_TYPE, METRIC_VALUE_CERT_DECISION_TYPE_UNKNOWN),
                            ]);
                        },
                        // 2.2 For DM - Update the decision with outcome + safepoint.
                        ChannelMessage::Decision(decision) => {
                            let talos_decision = decision.message.decision.to_string();
                            let version = decision.message.get_candidate_version();
                            let candidate_published = decision.message.metrics.candidate_published;
                            let decision_received_in_replicator = OffsetDateTime::now_utc().unix_timestamp_nanos();
                            info!(
                                "[Version>>{version}] decision recieved in replicator service {:?}ms. Time diff from candidate published to receive decision in replicator = {:?}ms ",
                                decision_received_in_replicator / 1_000_000_i128,
                                (decision_received_in_replicator - candidate_published) / 1_000_000_i128
                            );
                            if let Some(trace_parent) = decision.get_trace_parent() {
                                let propagated_context = PropagatedSpanContextData::new_with_trace_parent(trace_parent);
                                let context = global::get_text_map_propagator(|propagator| {
                                    propagator.extract(&propagated_context)
                                });
                                let linked_context = context.span().span_context().clone();
                                let span = tracing::info_span!("replicator_receive_decision", xid = decision.message.xid.clone());
                                span.add_link(linked_context);

                                self.replicator.process_decision_message(decision.decision_version, decision.message).instrument(span).await;
                            } else {
                                self.replicator.process_decision_message(decision.decision_version, decision.message).await;
                            }

                            self.metrics.record_consumed_offset(decision.decision_version, &[
                                KeyValue::new(METRIC_KEY_CERT_MESSAGE_TYPE, METRIC_VALUE_CERT_MESSAGE_TYPE_DECISION),
                                KeyValue::new(METRIC_KEY_CERT_DECISION_TYPE, talos_decision),
                            ],);


                            // Get a batch of remaining versions with their statemaps to install.
                            let statemaps_batch = self.replicator.generate_statemap_batch();

                            self.metrics.record_statemap_tx_count((self.statemaps_tx.max_capacity() - self.statemaps_tx.capacity()) as u64);

                            // Send statemaps batch to
                            for (ver, statemap_vec) in statemaps_batch {
                                self.statemaps_tx.send(StatemapQueueChannelMessage::Message((ver, statemap_vec))).await.unwrap();
                            }

                        },
                        ChannelMessage::Reset => {
                            debug!("Channel reset message received.");
                        }
                    }
                }
            }
            // // Commit offsets at interval.
            // _ = self.interval.tick() => {
            //     // If last item on suffix, we can commit till it's decision
            //     let suffix_length = self.replicator.suffix.get_suffix_len();
            //     let last_suffix_index = if suffix_length > 0 { suffix_length - 1 } else { 0 };

            //     if let Some(next_commit_offset) = self.replicator.next_commit_offset {
            //         if let Some(index) = self.replicator.suffix.get_index_from_head(next_commit_offset) {

            //             if index == last_suffix_index {
            //                 if let Ok(Some(c)) = self.replicator.suffix.get(next_commit_offset){
            //                     if let Some(decision_version) = c.decision_ver {
            //                         self.replicator.prepare_offset_for_commit(decision_version);
            //                     }
            //                 }
            //             }
            //         }
            //     }

            //     self.replicator.commit().await;

            // }
            // Receive feedback from installer.
            res = self.replicator_rx.recv() => {
                match res {
                    //
                    Some(ReplicatorChannel::LastInstalledVersion(version)) => {

                        if let Some(index) = self.replicator.suffix.update_prune_index(version) {
                            if let Err(err) = self.replicator.suffix.prune_till_index(index){
                                let item = self.replicator.suffix.get_by_index(index);
                                if let Some(suffix_item) = item {
                                    let ver = suffix_item.item_ver;
                                    error!("Failed to prune suffix till version {ver} and index {index}. Suffix head is at {}. Error {:?}", self.replicator.suffix.get_meta().head, err);
                                } else {
                                    error!("Failed to prune suffix till index {index}. Suffix head is at {}. Error {:?}", self.replicator.suffix.get_meta().head, err);
                                }
                            } else {
                                info!("Completed pruning suffix. New head = {} | Last installed version received = {version} | Remaining items on suffix = {:?} ", self.replicator.suffix.get_meta().head, self.replicator.suffix.get_suffix_len());
                            }
                        }
                    },
                    Some(ReplicatorChannel::SnapshotUpdatedVersion(version)) => {
                        self.replicator.prepare_offset_for_commit(version);

                        self.replicator.commit().await;

                    },
                    None => {},
                };

            }
        }

        Ok(())
    }

    pub async fn run(&mut self) -> Result<(), ReplicatorError> {
        info!("Starting Replicator Service.... ");

        loop {
            self.run_once().await?;
        }
    }
}

// $coverage:ignore-end
