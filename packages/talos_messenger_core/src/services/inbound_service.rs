use std::time::Duration;

use ahash::HashMap;
use async_trait::async_trait;
use log::{debug, error, info, warn};

use talos_certifier::{model::DecisionMessageTrait, ports::MessageReciever, ChannelMessage};
use talos_suffix::{core::SuffixMeta, Suffix, SuffixTrait};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::{
    sync::mpsc::{self},
    time::{timeout, Instant, Interval},
};

use crate::{
    core::{MessengerChannelFeedback, MessengerCommitActions, MessengerSystemService},
    errors::{MessengerServiceError, MessengerServiceErrorKind, MessengerServiceResult},
    models::MessengerCandidateMessage,
    suffix::{
        MessengerCandidate, MessengerStateTransitionTimestamps, MessengerSuffixItemTrait, MessengerSuffixTrait, SuffixItemCompleteStateReason, SuffixItemState,
    },
    utlis::get_allowed_commit_actions,
};

#[derive(Debug)]
pub struct MessengerInboundServiceConfig {
    /// commit size decides when the offsets can be committed.
    /// When the number of feedbacks is greater than the commit_size, a commit is issued.
    /// Default value is 5_000. Updating this value can impact the latency/throughput due to the frequency at which the commits will be issued.
    commit_size: u32,
    /// Frequency at which to commit should be done in ms
    commit_frequency: u32,
    /// The allowed on_commit actions
    allowed_actions: HashMap<String, Vec<String>>,
}

impl MessengerInboundServiceConfig {
    pub fn new(allowed_actions: HashMap<String, Vec<String>>, commit_size: Option<u32>, commit_frequency: Option<u32>) -> Self {
        Self {
            allowed_actions,
            commit_size: commit_size.unwrap_or(5_000),
            commit_frequency: commit_frequency.unwrap_or(10 * 1_000),
        }
    }
}

pub struct MessengerInboundService<M>
where
    M: MessageReciever<Message = ChannelMessage<MessengerCandidateMessage>> + Send + Sync + 'static,
{
    pub message_receiver: M,
    pub tx_actions_channel: mpsc::Sender<MessengerCommitActions>,
    pub rx_feedback_channel: mpsc::Receiver<MessengerChannelFeedback>,
    pub suffix: Suffix<MessengerCandidate>,
    pub config: MessengerInboundServiceConfig,
    // Interval at which the commit interval arm should execute.
    commit_interval: Interval,
    /// The last version/offset sent for commit. This property shows the version that was last committed, unlike `next_version_to_commit`, which is next to commit.
    last_committed_version: u64,
    /// The next version ready to be send for commit.
    next_version_to_commit: u64,
    ///
    timeout_ms: u64,
    ///
    feedback_buffer: Vec<MessengerChannelFeedback>,
}

impl<M> MessengerInboundService<M>
where
    M: MessageReciever<Message = ChannelMessage<MessengerCandidateMessage>> + Send + Sync + 'static,
{
    pub fn new(
        message_receiver: M,
        tx_actions_channel: mpsc::Sender<MessengerCommitActions>,
        rx_feedback_channel: mpsc::Receiver<MessengerChannelFeedback>,
        suffix: Suffix<MessengerCandidate>,
        config: MessengerInboundServiceConfig,
    ) -> Self {
        let commit_interval = tokio::time::interval(Duration::from_millis(config.commit_frequency as u64));
        let buffer_capacity = rx_feedback_channel.max_capacity();
        Self {
            message_receiver,
            tx_actions_channel,
            rx_feedback_channel,
            suffix,
            config,
            commit_interval,
            last_committed_version: 0,
            next_version_to_commit: 0,
            timeout_ms: 0,
            feedback_buffer: Vec::with_capacity(buffer_capacity),
        }
    }
    /// Get next versions with their commit actions to process.
    ///
    async fn process_next_actions(&mut self) -> MessengerServiceResult {
        let items_to_process = self.suffix.get_suffix_items_to_process();

        info!("Total next actions to process... {}", items_to_process.len());

        for mut payload in items_to_process {
            let ver = payload.version;

            // let mut headers = payload.headers;
            let timestamp = OffsetDateTime::now_utc().format(&Rfc3339).ok().unwrap();
            payload
                .headers
                .insert(MessengerStateTransitionTimestamps::StartOnCommitActions.to_string(), timestamp);

            // send for publishing

            // There is no point in sending more messages only for it to fail.
            if self.tx_actions_channel.capacity() == 0 {
                let timeout_error = MessengerServiceError {
                    kind: crate::errors::MessengerServiceErrorKind::TimedOut,
                    reason: "Actions channel is at capacity. Timeout out sending actions".to_string(),
                    data: Some(ver.to_string()),
                    service: "Inbound Service".to_string(),
                };
                warn!(
                    "We are already at max capacity in actions channel. Current capacity = {} | max capacity = {} ",
                    self.tx_actions_channel.capacity(),
                    self.tx_actions_channel.max_capacity()
                );
                break;
            } else {
                match tokio::time::timeout(
                    Duration::from_millis(30), //TODO: GK - make this configurable.
                    self.tx_actions_channel.send(payload),
                )
                .await
                {
                    Ok(res) => match res {
                        Ok(_) => {
                            self.suffix.set_item_state(ver, SuffixItemState::Processing);
                        }
                        Err(err) => {
                            error!("Failed sending actions for version {ver} with error {err:?}");
                            let send_error = MessengerServiceError {
                                kind: crate::errors::MessengerServiceErrorKind::Channel,
                                reason: err.to_string(),
                                data: Some(ver.to_string()),
                                service: "Inbound Service".to_string(),
                            };
                            return Err(send_error);
                        }
                    },
                    Err(_) => {
                        //TODO: GK - Potential to timeout would be because the channel is full and we aren't able to push new messages. This would be ideal place to enable backpressure.
                        warn!("Timed out waiting to send on_commit actions for version {ver}.");
                        let timeout_error = MessengerServiceError {
                            kind: crate::errors::MessengerServiceErrorKind::TimedOut,
                            reason: "Timeout out sending actions".to_string(),
                            data: Some(ver.to_string()),
                            service: "Inbound Service".to_string(),
                        };
                        break;
                    }
                }
            }

            // {
            //     error!("Failed sending actions for version {ver} with error {err:?}");
            //     // return Err(err);
            // } else {
            // };
            // if let Err(err) = self.tx_actions_channel.send(payload).await.map_err(|e| MessengerServiceError {
            //     kind: crate::errors::MessengerServiceErrorKind::Channel,
            //     reason: e.to_string(),
            //     data: Some(ver.to_string()),
            //     service: "Inbound Service".to_string(),
            // }) {
            //     error!("Failed sending actions for version {ver} with error {err:?}");
            //     return Err(err);
            // };

            // Mark item as in process
            info!("Finished sending next actions");
        }

        Ok(())
    }

    fn update_commit_offset(&mut self, version: u64) {
        let last_offset = self.next_version_to_commit;
        if version.gt(&last_offset) {
            if let Err(err) = self.message_receiver.update_offset_to_commit(version as i64) {
                error!("Failed updating offset from {last_offset:?} -> {version} with error {err:?}");
            } else {
                self.next_version_to_commit = version;
                debug!("Commit offset updated from {last_offset:?} -> {version}");
            }
        } else {
            debug!(
                "Skipped updating commit offset as version ({version}) is less than the last committed version ({:?})",
                last_offset
            );
        }
    }

    pub(crate) fn suffix_pruning(&mut self) {
        // Check prune eligibility by looking at the prune meta info.
        if let Some(index_to_prune) = self.suffix.get_safe_prune_index() {
            // error!("Pruning till index {index_to_prune}");
            // Call prune method on suffix.
            if let Ok(pruned_items) = self.suffix.prune_till_index(index_to_prune) {
                drop(pruned_items);
            }
            debug!(
                "[Suffix Pruning] suffix after prune printed as tuple of (index, ver, decsision_ver) \n\n {:?}",
                self.suffix.retrieve_all_some_vec_items()
            );
        }
    }

    /// Checks if all actions are completed and updates the state of the item to `Processed`.
    /// Also, checks if the suffix can be pruned and the message_receiver can be committed.
    pub(crate) fn check_and_update_all_actions_complete(&mut self, version: u64, reason: SuffixItemCompleteStateReason) {
        match self.suffix.are_all_actions_complete_for_version(version) {
            Ok(is_completed) if is_completed => {
                // info!(
                //     "All actions for version {version} is complete due to reason {reason:?} >> is it complete? {:?}",
                //     self.suffix.are_all_actions_complete_for_version(version)
                // );
                self.suffix.set_item_state(version, SuffixItemState::Complete(reason));

                // self.all_completed_versions.push(version);

                // if let Some((_, new_prune_version)) = self.suffix.update_prune_index_from_version(version) {
                //     self.update_commit_offset(new_prune_version);
                // }

                debug!("[Actions] All actions for version {version} completed!");
            }
            _ => {}
        }
    }
    ///
    /// Handles the failed to process feedback received from other services
    ///
    pub(crate) fn handle_action_failed(&mut self, version: u64, action_key: &str) {
        let item_state = self.suffix.get_item_state(version);
        match item_state {
            Some(SuffixItemState::Processing) | Some(SuffixItemState::PartiallyComplete) => {
                self.suffix
                    .set_item_state(version, SuffixItemState::Complete(SuffixItemCompleteStateReason::ErrorProcessing));

                self.suffix.increment_item_action_count(version, action_key);
                self.check_and_update_all_actions_complete(version, SuffixItemCompleteStateReason::ErrorProcessing);
                debug!(
                    "[Action] State version={version} changed from {item_state:?} => {:?}",
                    self.suffix.get_item_state(version)
                );
            }
            // If there was atleast 1 error, mark the final state as `Complete(ErrorProcessing)`.
            Some(SuffixItemState::Complete(..)) => {
                self.suffix
                    .set_item_state(version, SuffixItemState::Complete(SuffixItemCompleteStateReason::ErrorProcessing));

                self.suffix.increment_item_action_count(version, action_key);
                self.check_and_update_all_actions_complete(version, SuffixItemCompleteStateReason::ErrorProcessing);
                debug!(
                    "[Action] State version={version} changed from {item_state:?} => {:?}",
                    self.suffix.get_item_state(version)
                );
            }
            _ => (),
        };
    }
    ///
    /// Handles the feedback received from other services when they have successfully processed the action.
    /// Will update the individual action for the count and completed flag and also update state of the suffix item.
    ///
    pub(crate) fn handle_action_success(&mut self, version: u64, action_key: &str) {
        let item_state = self.suffix.get_item_state(version);
        match item_state {
            Some(SuffixItemState::Processing) | Some(SuffixItemState::PartiallyComplete) => {
                self.suffix.set_item_state(version, SuffixItemState::PartiallyComplete);

                self.suffix.increment_item_action_count(version, action_key);
                self.check_and_update_all_actions_complete(version, SuffixItemCompleteStateReason::Processed);
                debug!(
                    "[Action] State version={version} changed from {item_state:?} => {:?}",
                    self.suffix.get_item_state(version)
                );
            }
            _ => (),
        };
    }

    pub async fn run_once(&mut self) -> MessengerServiceResult {
        // When both the actions and feedbacks channel are at max_capacity. We could potential reach
        // a DEADLOCK if we continue to try consume new messages. Therefore we introduce a timeout so that
        // the feedback arm gets sufficient time to process the records.
        if self.rx_feedback_channel.capacity() == 0
        // ||self.tx_actions_channel.capacity() <= 100
        {
            // TODO: GK - Avoid hardcoded value.
            self.timeout_ms = 5;
            info!("Timeout set to {}ms.", self.timeout_ms);
        } else {
            self.timeout_ms = 0;
        }

        let current_feedbacks = self.rx_feedback_channel.max_capacity() - self.rx_feedback_channel.capacity();
        let feedback_limit = current_feedbacks.max(100);
        tokio::select! {
            // Receive feedback from publisher.
            _result = self.rx_feedback_channel.recv_many(&mut self.feedback_buffer,feedback_limit) => {
                // // error!("Feedback received is.. {feedback_result:?} | Remaining feedbacks = {}",self.rx_feedback_channel.max_capacity()-self.rx_feedback_channel.capacity());
                let mut last_version = None;
                let feedback_buffer = self.feedback_buffer.clone();
                for feedback_result in feedback_buffer {
                    match feedback_result {
                        MessengerChannelFeedback::Error(version, key, message_error) => {
                            error!("Failed to process version={version} with error={message_error:?}");
                            self.handle_action_failed(version, &key);
                            last_version = Some(version);

                        },
                        MessengerChannelFeedback::Success(version, key) => {
                            debug!("Successfully processed version={version} with action_key={key}");
                            self.handle_action_success(version, &key);
                            last_version = Some(version);
                        },
                    }
                }

                if let Some(version) = last_version {
                    if let Some((_, new_prune_version)) = self.suffix.update_prune_index_from_version(version) {
                        self.update_commit_offset(new_prune_version);
                    }
                }
                self.feedback_buffer.clear();

            }
            // Some(feedback_result) = self.rx_feedback_channel.recv() => {
            //     // error!("Feedback received is.. {feedback_result:?} | Remaining feedbacks = {}",self.rx_feedback_channel.max_capacity()-self.rx_feedback_channel.capacity());
            //     match feedback_result {
            //         MessengerChannelFeedback::Error(version, key, message_error) => {
            //             error!("Failed to process version={version} with error={message_error:?}");
            //             self.handle_action_failed(version, &key);

            //         },
            //         MessengerChannelFeedback::Success(version, key) => {
            //             debug!("Successfully processed version={version} with action_key={key}");
            //             self.handle_action_success(version, &key);
            //         },
            //     }

            // }
            // 1. Consume message.
            // Ok(Some(msg)) = self.message_receiver.consume_message() => {
            reciever_result = self.message_receiver.consume_message_with_timeout(self.timeout_ms) => {
                //TODO: GK - Should use Option instead?
                match reciever_result {
                    // 2.1 For CM - Install messages on the version
                    Ok(Some(ChannelMessage::Candidate(candidate))) => {
                        let version = candidate.message.version;
                        debug!("Candidate version received is {version}");
                        if version > 0 {
                            // insert item to suffix
                            if let Err(insert_error) = self.suffix.insert(version, candidate.message.into()) {
                                warn!("Failed to insert version {version} into suffix with error {insert_error:?}");
                            }

                            if let Some(item_to_update) = self.suffix.get_mut(version){
                                if let Some(commit_actions) = &item_to_update.item.candidate.on_commit {
                                    let filter_actions = get_allowed_commit_actions(commit_actions, &self.config.allowed_actions);
                                    if filter_actions.is_empty() {
                                        // There are on_commit actions, but not the ones required by messenger
                                        item_to_update.item.set_state(SuffixItemState::Complete(SuffixItemCompleteStateReason::NoRelavantCommitActions));
                                    } else {
                                        item_to_update.item.set_commit_action(filter_actions);
                                    }
                                } else {
                                    //  No on_commit actions
                                    item_to_update.item.set_state(SuffixItemState::Complete(SuffixItemCompleteStateReason::NoCommitActions));

                                }
                            };



                        } else {
                            warn!("Version 0 will not be inserted into suffix.")
                        }
                    },
                    // 2.2 For DM - Update the decision with outcome + safepoint.
                    Ok(Some(ChannelMessage::Decision(decision))) => {
                        let version = decision.message.get_candidate_version();
                        debug!("[Decision Message] Decision version received = {} for candidate version = {}", decision.decision_version, version);

                        // TODO: GK - no hardcoded filters on headers
                        let headers: HashMap<String, String> = decision.headers.into_iter().filter(|(key, _)| key.as_str() != "messageType").collect();
                        self.suffix.update_item_decision(version, decision.decision_version, &decision.message, headers);

                        // Look for any early `Complete(..)` state, and update the `prune_index` and `commit_offset`.
                        if let Ok(Some(suffix_item)) = self.suffix.get(version){
                            if matches!(suffix_item.item.get_state(), SuffixItemState::Complete(..)) {
                                if let Some((_, new_prune_version)) = self.suffix.update_prune_index_from_version(version) {
                                    self.update_commit_offset(new_prune_version);
                                }
                            }

                        };


                        // Pick the next items from suffix whose actions are to be processed.
                        // Timeout error happens when the channel is full. Therefore we sleep before consuming the next message,
                        // so that we don't reach a DEADLOCK.
                        if let Err(err) = self.process_next_actions().await{
                            if err.kind == MessengerServiceErrorKind::TimedOut {
                                //TODO: GK - no harded coded value here...
                                self.timeout_ms = 10;
                            }
                            return Err(err);
                        };


                    },
                    Ok(None) => {
                        debug!("No message to process..");
                    },
                    Err(error) => {
                        // Catch the error propogated, and if it has a version, mark the item as completed.
                        if let Some(version) = error.version {
                            if let Some(item_to_update) = self.suffix.get_mut(version){
                                item_to_update.item.set_state(SuffixItemState::Complete(SuffixItemCompleteStateReason::ErrorProcessing));
                            }
                        }
                        error!("error consuming message....{:?}", error);
                    },
                }

                warn!("Actions in channel = {} | Feedbacks in channels {}", self.tx_actions_channel.max_capacity()-self.tx_actions_channel.capacity(), self.rx_feedback_channel.max_capacity()-self.rx_feedback_channel.capacity());

            }
            // Periodically check and update the commit frequency and prune index.
            _ = self.commit_interval.tick() => {

                // error!("Feedback received is.. {feedback_result:?} | Remaining feedbacks = {}",self.rx_feedback_channel.max_capacity()-self.rx_feedback_channel.capacity());
                // if !self.feedback_buffer.is_empty() {
                //     let start_ms = Instant::now();
                //     let buffer_len = self.feedback_buffer.len();
                //     let mut last_version = None;
                //     let feedback_buffer = self.feedback_buffer.clone();
                //     for feedback_result in feedback_buffer {
                //         match feedback_result {
                //             MessengerChannelFeedback::Error(version, key, message_error) => {
                //                 error!("Failed to process version={version} with error={message_error:?}");
                //                 self.handle_action_failed(version, &key);
                //                 last_version = Some(version);

                //             },
                //             MessengerChannelFeedback::Success(version, key) => {
                //                 debug!("Successfully processed version={version} with action_key={key}");
                //                 self.handle_action_success(version, &key);
                //                 last_version = Some(version);
                //             },
                //         }
                //     }

                //     if let Some(version) = last_version {
                //         if let Some((_, new_prune_version)) = self.suffix.update_prune_index_from_version(version) {
                //             self.update_commit_offset(new_prune_version);
                //         }
                //     }
                //     self.feedback_buffer.clear();
                //     warn!("Updating suffix using the feedback. Total feedbacks processed = {buffer_len}  in {} ms", start_ms.elapsed().as_millis());
                // }

                if !self.suffix.messages.is_empty(){
                    if let  Some(Some(last_item_on_suffix)) = self.suffix.messages.back() {
                        let last_version = last_item_on_suffix.item_ver;
                        let last_version_decision_ver = last_item_on_suffix.decision_ver;
                        let last_committed_version = self.last_committed_version;

                        let version_ready_to_commit = self.next_version_to_commit;

                        if version_ready_to_commit.gt(&last_committed_version) {
                            // There are more items in suffix.
                            if let Some((_, new_prune_version)) = self.suffix.update_prune_index_from_version(last_version) {

                                // If the new_prune_version is same as the last item on suffix, then is is safe to commit till the decision offset of that message.
                                let new_offset = if new_prune_version.eq(&last_version) {
                                    if let Some(decision_vers) = last_version_decision_ver {
                                        debug!("Updating to decision offset ({:?})", decision_vers);

                                        // error!("Resetting suffix to initial state");
                                        // self.suffix.reset();
                                        decision_vers
                                    } else {
                                        new_prune_version
                                    }

                                } else {
                                    debug!("Updating to candidate offset ({})", new_prune_version);
                                    new_prune_version
                                };
                                self.update_commit_offset(new_offset);
                                match self.message_receiver.commit() {
                                    Err(err) => {
                                        error!("[Commit] Error committing {err:?}");
                                    }
                                    Ok(_) => {
                                        info!("Committed to new offset {new_offset}");
                                        self.last_committed_version = new_offset;
                                    },
                                }
                            };

                        }

                    }
                }

            }
            else => {
                warn!("Unhandled arm....");
            }

        }

        // NOTE: Pruning and committing offset adds to latency if done more frequently.
        if (self.next_version_to_commit - self.last_committed_version).ge(&(self.config.commit_size as u64)) {
            match self.message_receiver.commit() {
                Err(err) => {
                    error!("[Commit] Error committing {err:?}");
                }
                Ok(_) => {
                    self.last_committed_version = self.next_version_to_commit;
                    info!("Last commit at offset {}", self.last_committed_version);
                }
            };
            // else {
            // }
        }

        // Update the prune index and commit
        let SuffixMeta {
            prune_index,
            prune_start_threshold,
            ..
        } = self.suffix.get_meta();

        if prune_index.gt(prune_start_threshold) {
            self.suffix_pruning();
        };

        Ok(())
    }
}

#[async_trait]
impl<M> MessengerSystemService for MessengerInboundService<M>
where
    M: MessageReciever<Message = ChannelMessage<MessengerCandidateMessage>> + Send + Sync + 'static,
{
    async fn start(&self) -> MessengerServiceResult {
        info!("Start Messenger service");
        Ok(())
    }

    async fn stop(&self) -> MessengerServiceResult {
        todo!()
    }

    async fn run(&mut self) -> MessengerServiceResult {
        info!("Running Messenger service");

        loop {
            self.run_once().await?;
        }
    }
}
