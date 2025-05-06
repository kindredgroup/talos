// $coverage:ignore-start
use std::{fmt::Debug, time::Duration};

use crate::{
    core::{Replicator, ReplicatorChannel},
    errors::ReplicatorError,
    models::{ReplicatorCandidate, ReplicatorCandidateMessage},
    suffix::ReplicatorSuffixTrait,
    StatemapQueueChannelMessage,
};

use opentelemetry::{global, trace::TraceContextExt, KeyValue};

use talos_certifier::{ports::MessageReciever, ChannelMessage};
use talos_common_utils::otel::{
    metric_constants::{
        METRIC_KEY_CERT_DECISION_TYPE, METRIC_KEY_CERT_MESSAGE_TYPE, METRIC_NAME_CERTIFICATION_OFFSET, METRIC_VALUE_CERT_DECISION_TYPE_UNKNOWN,
        METRIC_VALUE_CERT_MESSAGE_TYPE_CANDIDATE, METRIC_VALUE_CERT_MESSAGE_TYPE_DECISION,
    },
    propagated_context::PropagatedSpanContextData,
};

use tokio::sync::mpsc;
use tracing::Instrument;
use tracing::{debug, error, info};
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub struct ReplicatorServiceConfig {
    /// Frequency in milliseconds
    pub commit_frequency_ms: u64,
    /// Enable internal stats
    pub enable_stats: bool,
}

pub async fn replicator_service<S, M>(
    statemaps_tx: mpsc::Sender<StatemapQueueChannelMessage>,
    mut replicator_rx: mpsc::Receiver<ReplicatorChannel>,
    mut replicator: Replicator<ReplicatorCandidate, S, M>,
    config: ReplicatorServiceConfig,
) -> Result<(), ReplicatorError>
where
    S: ReplicatorSuffixTrait<ReplicatorCandidate> + Debug,
    M: MessageReciever<Message = ChannelMessage<ReplicatorCandidateMessage>> + Send + Sync,
{
    info!("Starting Replicator Service.... ");
    let mut interval = tokio::time::interval(Duration::from_millis(config.commit_frequency_ms));

    let g_statemaps_tx = global::meter("sdk_replicator").u64_gauge("repl_statemaps_channel").with_unit("items").build();
    let g_consumed_offset = global::meter("sdk_replicator")
        .u64_gauge(format!("repl_{}", METRIC_NAME_CERTIFICATION_OFFSET))
        .build();

    let channel_size = 100_000_u64;

    loop {
        tokio::select! {
        // 1. Consume message.
        res = replicator.receiver.consume_message() => {
            if let Ok(Some(msg)) = res {

                // 2. Add/update to suffix.
                match msg {
                    // 2.1 For CM - Install messages on the version
                    ChannelMessage::Candidate(candidate) => {
                        let offset = candidate.message.version;
                        replicator.process_consumer_message(offset, candidate.message.into()).await;
                        g_consumed_offset.record(
                            offset,
                            &[
                                KeyValue::new(METRIC_KEY_CERT_MESSAGE_TYPE, METRIC_VALUE_CERT_MESSAGE_TYPE_CANDIDATE),
                                KeyValue::new(METRIC_KEY_CERT_DECISION_TYPE, METRIC_VALUE_CERT_DECISION_TYPE_UNKNOWN),
                            ],
                        );
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

                            replicator.process_decision_message(decision.decision_version, decision.message).instrument(span).await;
                        } else {
                            replicator.process_decision_message(decision.decision_version, decision.message).await;
                        }

                        g_consumed_offset.record(
                            decision.decision_version,
                            &[
                                KeyValue::new(METRIC_KEY_CERT_MESSAGE_TYPE, METRIC_VALUE_CERT_MESSAGE_TYPE_DECISION),
                                KeyValue::new(METRIC_KEY_CERT_DECISION_TYPE, talos_decision),
                            ],
                        );

                        // Get a batch of remaining versions with their statemaps to install.
                        let statemaps_batch = replicator.generate_statemap_batch();

                        g_statemaps_tx.record(channel_size - statemaps_tx.capacity() as u64, &[]);

                        // Send statemaps batch to
                        for (ver, statemap_vec) in statemaps_batch {
                            statemaps_tx.send(StatemapQueueChannelMessage::Message((ver, statemap_vec))).await.unwrap();
                        }

                    },
                    ChannelMessage::Reset => {
                        debug!("Channel reset message received.");
                    }
                }
            }
        }
        // Commit offsets at interval.
        _ = interval.tick() => {
            // If last item on suffix, we can commit till it's decision
            let suffix_length = replicator.suffix.get_suffix_len();
            let last_suffix_index = if suffix_length > 0 { suffix_length - 1 } else { 0 };

            if let Some(next_commit_offset) = replicator.next_commit_offset {
                if let Some(index) = replicator.suffix.get_index_from_head(next_commit_offset) {

                    if index == last_suffix_index {
                        if let Ok(Some(c)) = replicator.suffix.get(next_commit_offset){
                            if let Some(decision_version) = c.decision_ver {
                                replicator.prepare_offset_for_commit(decision_version);
                            }
                        }
                    }
                }
            }

            replicator.commit().await;

        }
        // Receive feedback from installer.
        res = replicator_rx.recv() => {
                if let Some(ReplicatorChannel::LastInstalledVersion(version)) = res {
                    if let Some(index) = replicator.suffix.update_prune_index(version) {
                        if let Err(err) = replicator.suffix.prune_till_index(index){
                            let item = replicator.suffix.get_by_index(index);
                            if let Some(suffix_item) = item {
                                let ver = suffix_item.item_ver;
                                error!("Failed to prune suffix till version {ver} and index {index}. Suffix head is at {}. Error {:?}", replicator.suffix.get_meta().head, err);
                            } else {
                                error!("Failed to prune suffix till index {index}. Suffix head is at {}. Error {:?}", replicator.suffix.get_meta().head, err);
                            }
                        } else {
                            replicator.prepare_offset_for_commit(version);
                            error!("Completed pruning suffix. New head = {} | Last installed version received = {version} | Remaining items on suffix = {:?} ", replicator.suffix.get_meta().head, replicator.suffix.get_suffix_len());
                        }
                    }
                }
            }

        }
    }
}

// $coverage:ignore-end
