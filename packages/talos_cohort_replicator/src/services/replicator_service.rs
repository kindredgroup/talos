// $coverage:ignore-start
use std::{fmt::Debug, time::Duration};

use crate::{
    core::{Replicator, ReplicatorChannel, StatemapItem},
    errors::ReplicatorError,
    models::{ReplicatorCandidate, ReplicatorCandidateMessage},
    suffix::ReplicatorSuffixTrait,
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
use time::OffsetDateTime;
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
    statemaps_tx: mpsc::Sender<(u64, Vec<StatemapItem>)>,
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

    let mut total_items_send = 0;
    let mut total_items_installed = 0;
    let mut time_first_item_created_start_ns: i128 = 0; //
    let mut time_last_item_send_end_ns: i128 = 0;
    let mut time_last_item_installed_ns: i128 = 0;

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

                        if total_items_send == 0 {
                            time_first_item_created_start_ns = OffsetDateTime::now_utc().unix_timestamp_nanos();
                        }
                        // Get a batch of remaining versions with their statemaps to install.
                        let statemaps_batch = replicator.generate_statemap_batch();

                        total_items_send += statemaps_batch.len();
                        g_statemaps_tx.record(channel_size - statemaps_tx.capacity() as u64, &[]);

                        // Send statemaps batch to
                        for (ver, statemap_vec) in statemaps_batch {
                            statemaps_tx.send((ver, statemap_vec)).await.unwrap();
                        }

                        // statemaps_tx

                        time_last_item_send_end_ns = OffsetDateTime::now_utc().unix_timestamp_nanos();
                    },
                    ChannelMessage::Reset => {
                        debug!("Channel reset message received.");
                    }
                }
            }
        }
        // Commit offsets at interval.
        _ = interval.tick() => {
            if config.enable_stats {
                let duration_sec = Duration::from_nanos((time_last_item_send_end_ns - time_first_item_created_start_ns) as u64).as_secs_f32();
                let tps_send = total_items_send as f32 / duration_sec;


                let duration_installed_sec = Duration::from_nanos((time_last_item_installed_ns - time_first_item_created_start_ns) as u64).as_secs_f32();
                let tps_install = total_items_installed as f32 / duration_installed_sec;
                // let tps_install_feedback =

                debug!("
                Replicator Stats:
                      send for install      : tps={tps_send:.3}    | count={total_items_send}
                      installed             : tps={tps_install:.3}    | count={total_items_installed}
                    \n ");
            }

            if let Some((new_offset, candidate_version, _candidate_index)) = replicator.compute_new_offset(){
                replicator.suffix.update_prune_index(candidate_version);
                replicator.prepare_offset_for_commit(new_offset).await;
                replicator.commit().await;
            }

        }
        // Receive feedback from installer.
        res = replicator_rx.recv() => {
                if let Some(result) = res {
                    match result {
                        // 4. Remove the versions if installations are complete.
                        ReplicatorChannel::InstallationSuccess(vers) => {

                            let version = vers.last().unwrap().to_owned();
                            debug!("Installated successfully till version={version:?}");
                            // Mark the suffix item as installed.
                            replicator.suffix.set_item_installed(version);

                            // // if all prior items are installed, then update the prune vers
                            let updated_prune_index = replicator.suffix.update_prune_index(version);

                            debug!("Replicator last_installing count = {} | is prune_index ({:?}) >= prune_start_threshold ({:?}) ==> {}", replicator.last_installing, replicator.suffix.get_suffix_meta().prune_index , replicator.suffix.get_suffix_meta().prune_start_threshold, replicator.suffix.get_suffix_meta().prune_index >= replicator.suffix.get_suffix_meta().prune_start_threshold);
                            // When the prune_index was updated, we know the following:
                            // - New prune_index > previous prune_index
                            // - It is safe to drop all items in suffix till the prune_index.
                            if let Some(index) = updated_prune_index {
                                if replicator.last_installing > 0 && updated_prune_index >= replicator.suffix.get_suffix_meta().prune_start_threshold {

                                    if let Err(err) = replicator.suffix.prune_till_index(index){
                                        let item = replicator.suffix.get_by_index(index);
                                        if let Some(suffix_item) = item {
                                            let ver = suffix_item.item_ver;
                                            error!("Failed to prune suffix till version {ver} and index {index}. Suffix head is at {}. Error {:?}", replicator.suffix.get_meta().head, err);
                                        } else {
                                            error!("Failed to prune suffix till index {index}. Suffix head is at {}. Error {:?}", replicator.suffix.get_meta().head, err);
                                        }
                                    } else {
                                        info!("Completed pruning suffix. New head = {} ", replicator.suffix.get_meta().head);
                                    }
                                };

                            }
                            total_items_installed += 1;
                            time_last_item_installed_ns = OffsetDateTime::now_utc().unix_timestamp_nanos();

                        }
                        ReplicatorChannel::InstallationFailure(err) => {
                            error!("Installation failed with error {err:?}");

                        }
                    }
                }
            }

        }
    }
}

// $coverage:ignore-end
