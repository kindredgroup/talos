use std::time::Duration;

use ahash::{HashMap, HashMapExt};
use async_trait::async_trait;
use log::{debug, error, info, warn};

use talos_certifier::{
    core::{CandidateChannelMessage, DecisionChannelMessage},
    model::DecisionMessageTrait,
    ports::{errors::MessageReceiverError, MessageReciever},
    ChannelMessage,
};
use talos_common_utils::back_pressure::TalosBackPressureConfig;
use talos_suffix::{core::SuffixMeta, Suffix, SuffixTrait};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::{
    sync::mpsc::{self},
    time::Interval,
};

use crate::{
    core::{MessengerChannelFeedback, MessengerCommitActions, MessengerSystemService},
    errors::{MessengerServiceError, MessengerServiceResult},
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
    /// Configs to check back pressure.
    back_pressure_configs: TalosBackPressureConfig,
}

pub async fn receive_new_message<M>(
    receiver: &mut M,
    is_back_pressure_enabled: bool,
) -> Option<Result<Option<ChannelMessage<MessengerCandidateMessage>>, MessageReceiverError>>
where
    M: MessageReciever<Message = ChannelMessage<MessengerCandidateMessage>> + Send + Sync + 'static,
{
    if is_back_pressure_enabled {
        // This sleep with hardcoded value is fine, this just introduces a delay for fairness so that other async arms of tokio select can execute.
        // We introduce this delay just so that feedback or any other arm gets a chance to execute when back-pressue is enabled.
        tokio::time::sleep(Duration::from_millis(2)).await;
        None
    } else {
        Some(receiver.consume_message().await)
    }
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
        back_pressure_configs: TalosBackPressureConfig,
    ) -> Self {
        let commit_interval = tokio::time::interval(Duration::from_millis(config.commit_frequency as u64));

        Self {
            message_receiver,
            tx_actions_channel,
            rx_feedback_channel,
            suffix,
            config,
            commit_interval,
            last_committed_version: 0,
            next_version_to_commit: 0,
            back_pressure_configs,
        }
    }
    /// Get next versions with their commit actions to process.
    ///
    async fn process_next_actions(&mut self) -> MessengerServiceResult<u32> {
        let max_new_items_to_pick = self.back_pressure_configs.get_remaining_count();

        let items_to_process = self.suffix.get_suffix_items_to_process(max_new_items_to_pick);
        let new_in_progress_count = items_to_process.len() as u32;

        for item in items_to_process {
            let ver = item.version;

            let mut headers = item.headers;
            let timestamp = OffsetDateTime::now_utc().format(&Rfc3339).ok().unwrap();
            headers.insert(MessengerStateTransitionTimestamps::StartOnCommitActions.to_string(), timestamp);

            let payload_to_send = MessengerCommitActions {
                version: ver,
                commit_actions: item.actions.iter().fold(HashMap::new(), |mut acc, (key, value)| {
                    acc.insert(key.to_string(), value.get_payload().clone());
                    acc
                }),
                headers,
            };
            // send for publishing

            self.tx_actions_channel.send(payload_to_send).await.map_err(|e| MessengerServiceError {
                kind: crate::errors::MessengerServiceErrorKind::Channel,
                reason: e.to_string(),
                data: Some(ver.to_string()),
                service: "Inbound Service".to_string(),
            })?;

            // Mark item as in process
            self.suffix.set_item_state(ver, SuffixItemState::Processing);
            // Increment the in_progress_count which is used to determine when back-pressure is enforced.
            self.back_pressure_configs.increment_current();
        }

        Ok(new_in_progress_count)
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
            let _ = self.suffix.prune_till_index(index_to_prune);
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
                self.suffix.set_item_state(version, SuffixItemState::Complete(reason));

                // self.all_completed_versions.push(version);

                // When any item moved to final state, we decrement this counter, which thereby relaxes the back-pressure if it was enforced.
                self.back_pressure_configs.decrement_current();

                if let Some((_, new_prune_version)) = self.suffix.update_prune_index_from_version(version) {
                    self.update_commit_offset(new_prune_version);
                }

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

    /// Process the feedback received from another thread for some `on_commit` action.
    pub(crate) fn process_feedbacks(&mut self, feedback_result: MessengerChannelFeedback) {
        match feedback_result {
            MessengerChannelFeedback::Error(version, key, message_error) => {
                error!("Failed to process version={version} with error={message_error:?}");
                self.handle_action_failed(version, &key);
            }
            MessengerChannelFeedback::Success(version, key) => {
                debug!("Successfully processed version={version} with action_key={key}");
                self.handle_action_success(version, &key);
            }
        }
    }

    /// Process the incoming candidate message
    ///     - Writes the candidate to suffix.
    ///     - Update to an early `Complete` state based on the `on_commit` actions.
    ///     - Transform the `on_commit` action to format required internally.
    pub(crate) fn process_candidate_message(&mut self, candidate: CandidateChannelMessage<MessengerCandidateMessage>) {
        let version = candidate.message.version;
        debug!("Candidate version received is {version}");
        if version > 0 {
            // insert item to suffix
            if let Err(insert_error) = self.suffix.insert(version, candidate.message.into()) {
                warn!("Failed to insert version {version} into suffix with error {insert_error:?}");
            }

            if let Some(item_to_update) = self.suffix.get_mut(version) {
                if let Some(commit_actions) = &item_to_update.item.candidate.on_commit {
                    let filter_actions = get_allowed_commit_actions(commit_actions, &self.config.allowed_actions);
                    if filter_actions.is_empty() {
                        // There are on_commit actions, but not the ones required by messenger
                        item_to_update
                            .item
                            .set_state(SuffixItemState::Complete(SuffixItemCompleteStateReason::NoRelavantCommitActions));
                    } else {
                        item_to_update.item.set_commit_action(filter_actions);
                    }
                } else {
                    //  No on_commit actions
                    item_to_update
                        .item
                        .set_state(SuffixItemState::Complete(SuffixItemCompleteStateReason::NoCommitActions));
                }
            };
        } else {
            warn!("Version 0 will not be inserted into suffix.")
        }
    }

    /// Process the incoming decision message
    ///     - Update the suffix candidate item with decision.
    ///     - If any type of early `Complete` state, update the `prune_index` and `commit_offset` if applicable.
    pub(crate) fn process_decision_message(&mut self, decision: DecisionChannelMessage) {
        let version = decision.message.get_candidate_version();
        debug!(
            "[Decision Message] Decision version received = {} for candidate version = {}",
            decision.decision_version, version
        );

        // TODO: GK - no hardcoded filters on headers
        let headers: HashMap<String, String> = decision.headers.into_iter().filter(|(key, _)| key.as_str() != "messageType").collect();
        self.suffix.update_item_decision(version, decision.decision_version, &decision.message, headers);

        // Look for any early `Complete(..)` state, and update the `prune_index` and `commit_offset`.
        if let Ok(Some(suffix_item)) = self.suffix.get(version) {
            if matches!(suffix_item.item.get_state(), SuffixItemState::Complete(..)) {
                if let Some((_, new_prune_version)) = self.suffix.update_prune_index_from_version(version) {
                    self.update_commit_offset(new_prune_version);
                }
            }
        };
    }

    /// Common house keeping checks and updates that run at the end of every iteration of `Self::run_once`.
    /// - Check if backpressure must be enabled.
    /// - Check if we have crossed the `commit_size` threshold and issue a commit.
    /// - Check if suffix should be pruned.
    pub(crate) fn run_once_housekeeping(&mut self) -> MessengerServiceResult {
        // check back pressure
        let enable_back_pressure = self.back_pressure_configs.should_apply_back_pressure();

        // update back pressure if it has changed.
        if self.back_pressure_configs.is_enabled != enable_back_pressure {
            if enable_back_pressure {
                info!(
                    "Applying back pressure as the total number of records currently being processed ({}) >= max in progress allowed ({:?})",
                    self.back_pressure_configs.current, self.back_pressure_configs.max_threshold
                );
            } else {
                info!(
                    "Removing back pressure as the total number of records currently being processed ({}) < max in progress allowed ({:?})",
                    self.back_pressure_configs.current, self.back_pressure_configs.max_threshold
                );
            }
            self.back_pressure_configs.update_back_pressure_flag(enable_back_pressure);
        }

        // NOTE: Pruning and committing offset adds to latency if done more frequently.

        // issue commit, is applicable.
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
        }

        // suffix prune, if applicable.
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

    pub async fn run_once(&mut self) -> MessengerServiceResult {
        tokio::select! {
            // feedback arm - Processes the feedbacks
            Some(feedback_result) = self.rx_feedback_channel.recv() => {
                self.process_feedbacks(feedback_result);
                self.process_next_actions().await?;

            }

            // Incoming message arm -
            //              - Processes the candidate or decision messages received.
            //              - Applies back pressure if too many records are being processed.
            //              - If back pressure is applied, will try to drain as many feedbacks and update the suffix as soon as possible to release the backpressure quickly.
            result = receive_new_message(&mut self.message_receiver, self.back_pressure_configs.is_enabled) => {
                if let Some(receiver_result) = result {
                    match receiver_result {
                        // 2.1 For CM - Install messages on the version
                        Ok(Some(ChannelMessage::Candidate(candidate))) => {
                           self.process_candidate_message(*candidate);
                        },
                        // 2.2 For DM - Update the decision with outcome + safepoint.
                        Ok(Some(ChannelMessage::Decision(decision))) => {

                            self.process_decision_message(*decision);
                            // Pick the next items from suffix whose actions are to be processed.
                            self.process_next_actions().await?;


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
                } else {
                    debug!("Not consuming new messages due to backpressure enabled. Current in progress count = {} | Total feedbacks available in the feedback channel = {} | total actions send in the actions channel = {}", self.back_pressure_configs.current ,self.rx_feedback_channel.max_capacity() - self.rx_feedback_channel.capacity(), self.tx_actions_channel.max_capacity() - self.tx_actions_channel.capacity());
                }
            }
            // Periodically check and update the commit frequency and prune index.
            _ = self.commit_interval.tick() => {
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

        self.run_once_housekeeping()?;

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
