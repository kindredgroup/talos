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

use talos_certifier::{
    ports::{ConsumeMessageTimeoutType, MessageReciever},
    ChannelMessage,
};
use talos_common_utils::{
    backpressure::{
        config::BackPressureConfig,
        controller::{BackPressureController, BackPressureVersionTracker},
    },
    otel::{
        metric_constants::{
            METRIC_KEY_CERT_DECISION_TYPE, METRIC_KEY_CERT_MESSAGE_TYPE, METRIC_NAME_CERTIFICATION_OFFSET, METRIC_VALUE_CERT_DECISION_TYPE_UNKNOWN,
            METRIC_VALUE_CERT_MESSAGE_TYPE_CANDIDATE, METRIC_VALUE_CERT_MESSAGE_TYPE_DECISION,
        },
        propagated_context::PropagatedSpanContextData,
    },
};

use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::{sync::mpsc, time::Interval};
use tracing::{debug, error, info};
use tracing::{warn, Instrument};
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
    backpressure_controller: BackPressureController,
    next_backpressure_check_ns: i128,
    max_allowed: u64,
    last_consume_timeout_type: ConsumeMessageTimeoutType,
    current_iter_at_max: u64,
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

        // TODO: GK - maybe use number of items for install as the criteria to determine the suffix size?
        let backpressure_config = BackPressureConfig::from_env();
        let backpressure_controller = BackPressureController::with_config(backpressure_config);

        Self {
            statemaps_tx,
            replicator_rx,
            replicator,
            config,
            metrics,
            backpressure_controller,
            next_backpressure_check_ns: 0,
            max_allowed: 5,
            current_iter_at_max: 0,
            last_consume_timeout_type: ConsumeMessageTimeoutType::NoTimeout,
            _interval: interval,
        }
    }

    pub async fn run_once(&mut self) -> Result<(), ReplicatorError> {
        let current_time_ns = OffsetDateTime::now_utc().unix_timestamp_nanos();

        // TODO: GK - Refactor this, and move some of the logic into the backpressue controller code.
        let backpressure_consume_timeout_type = if current_time_ns >= self.next_backpressure_check_ns {
            warn!("[replicator_service] Starting to compute dynamic back pressure");

            let current_suffix_length = self.replicator.suffix.get_suffix_len() as u64;
            let backpressure_timeout_ms = self.backpressure_controller.compute_backpressure_timeout(current_suffix_length);

            if backpressure_timeout_ms > 0 {
                warn!("[replicator_service] backpressure_timeout_ms = {backpressure_timeout_ms} | current_suffix_length = {current_suffix_length} | current_suffix_head = {}", self.replicator.suffix.get_meta().head);
                self.backpressure_controller.reset_version_trackers();
            }
            // Set the number of times the timeout is at max.
            if backpressure_timeout_ms >= self.backpressure_controller.config.max_timeout_ms {
                self.current_iter_at_max += 1;
            } else {
                self.current_iter_at_max = 0;
            }

            let mut backpressure_timeout_type: ConsumeMessageTimeoutType = ConsumeMessageTimeoutType::NoTimeout;
            // Check current against max
            if self.current_iter_at_max >= self.max_allowed && self.current_iter_at_max < (self.max_allowed + 3) {
                warn!(
                    "[replicator_service] At max timeout for back pressure current_iter_at_max = {} | max_allowed = {} ",
                    self.current_iter_at_max, self.max_allowed
                );
                backpressure_timeout_type = ConsumeMessageTimeoutType::SteadyAtMax(self.current_iter_at_max);
            } else {
                if backpressure_timeout_ms > 0 {
                    backpressure_timeout_type = ConsumeMessageTimeoutType::Timeout(backpressure_timeout_ms);
                    self.current_iter_at_max = 0;
                }
            }
            self.next_backpressure_check_ns = current_time_ns + (10 * 1_000_000) as i128;

            self.last_consume_timeout_type = backpressure_timeout_type.clone();
            backpressure_timeout_type
        } else {
            let dt = OffsetDateTime::UNIX_EPOCH + Duration::from_nanos(self.next_backpressure_check_ns as u64);
            let timestamp = dt.format(&Rfc3339).unwrap();
            warn!(
                "[replicator_service] Else block - Not computing dynamic pressure, next compute is only at {:?}",
                timestamp
            );
            let decay_rate = 0.8; // 20% from last.
            let new_consume_timeout_type = match self.last_consume_timeout_type {
                ConsumeMessageTimeoutType::NoTimeout => ConsumeMessageTimeoutType::NoTimeout,
                ConsumeMessageTimeoutType::Timeout(time_ms) => {
                    let new_time_ms = ((time_ms as f64 * decay_rate).round() as u64).max(self.backpressure_controller.config.min_timeout_ms);
                    ConsumeMessageTimeoutType::Timeout(new_time_ms)
                }
                ConsumeMessageTimeoutType::SteadyAtMax(k) => ConsumeMessageTimeoutType::SteadyAtMax(k),
            };
            self.last_consume_timeout_type = new_consume_timeout_type;

            self.last_consume_timeout_type.clone()
        };
        // let backpressure_timeout_ms = 0;

        tokio::select! {
            // 1. Consume message.
            res = self.replicator.receiver.consume_message_with_timeout(backpressure_consume_timeout_type) => {
                if let Ok(Some(msg)) = res {
                    // 2. Add/update to suffix.
                    match msg {
                        // 2.1 For CM - Install messages on the version
                        ChannelMessage::Candidate(candidate) => {
                            let offset = candidate.message.version;
                            self.replicator.process_consumer_message(offset, candidate.message.into()).await;

                            self.backpressure_controller.update_candidate_received_tracker(BackPressureVersionTracker{version: offset, time_ns: OffsetDateTime::now_utc().unix_timestamp_nanos()});

                            self.metrics.record_consumed_offset(offset, &[
                                KeyValue::new(METRIC_KEY_CERT_MESSAGE_TYPE, METRIC_VALUE_CERT_MESSAGE_TYPE_CANDIDATE),
                                KeyValue::new(METRIC_KEY_CERT_DECISION_TYPE, METRIC_VALUE_CERT_DECISION_TYPE_UNKNOWN),
                            ]);
                        },
                        // 2.2 For DM - Update the decision with outcome + safepoint.
                        ChannelMessage::Decision(decision) => {
                            let talos_decision = decision.message.decision.to_string();
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
                                self.backpressure_controller.update_suffix_head_trackers(BackPressureVersionTracker { version: self.replicator.suffix.get_meta().head, time_ns: OffsetDateTime::now_utc().unix_timestamp_nanos() });
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
        info!("Backpressure (early test) mode ");

        loop {
            self.run_once().await?;
        }
    }
}

// $coverage:ignore-end
