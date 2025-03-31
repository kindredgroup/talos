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
    // Flag denotes if back pressure is enabled, and therefore the thread cannot receive more messages (candidate/decisions) to process.
    // enable_back_pressure: bool,
    /// Number of items currently in progress. Which denotes, the number of items in suffix in `SuffixItemState::Processing` or `SuffixItemState::PartiallyComplete`.
    in_progress_count: u32,
    /// Configs to check back pressure.
    back_pressure_configs: TalosBackPressureConfig,
}

#[derive(Debug, Default, Clone)]
pub struct TalosBackPressureConfig {
    /// Flag denotes if back pressure is enabled, and therefore the thread cannot receive more messages (candidate/decisions) to process.
    pub is_enabled: bool,
    /// Max count before back pressue is enabled.
    /// if `None`, back pressure logic will not apply.
    max_threshold: Option<u32>,
    /// `min_threshold` helps to prevent immediate toggle between switch on and off of the backpressure.
    /// Batch of items to process, when back pressure is enabled before disable logic is checked?
    /// if None, no minimum check is done, and as soon as the count is below the max_threshold, back pressure is disabled.
    min_threshold: Option<u32>,
}

impl TalosBackPressureConfig {
    pub fn new(min_threshold: Option<u32>, max_threshold: Option<u32>) -> Self {
        assert!(
            min_threshold.le(&max_threshold),
            "min_threshold ({min_threshold:?}) must be less than max_threshold ({max_threshold:?})"
        );
        Self {
            max_threshold,
            min_threshold,
            is_enabled: false,
        }
    }

    /// Get the remaining available count before hitting the max_threshold, and thereby enabling back pressure.
    pub fn get_remaining_count(&self, current_count: u32) -> Option<u32> {
        self.max_threshold.map(|max| max.saturating_sub(current_count))
    }

    /// Looks at the `max_threshold` and `min_threshold` to determine if back pressure should be enabled. `max_threshold` is used to determine when to enable the back-pressure
    /// whereas, `min_threshold` is used to look at the lower bound
    /// - `max_threshold` - Use to determine the upper bound of maximum items allowed.
    ///                     If this is set to `None`, no back pressure will be applied.
    ///                     `max_threshold` is
    /// - `min_threshold` - Use to determine the lower bound of maximum items allowed. If this is set to `None`, no back pressure will be applied.
    pub fn should_enable_back_pressure(&mut self, current_count: u32) -> bool {
        match self.max_threshold {
            Some(max_threshold) => {
                // if not enabled, only check against the max_threshold.
                if current_count >= max_threshold {
                    true
                } else {
                    // if already enabled, check when is it safe to remove.

                    // If there is Some(`min_threshold`), then return true if current_count > `min_threshold`.
                    // If there is Some(`min_threshold`), then return false if current_count <= `min_threshold`.
                    // If None, then return false.
                    match self.min_threshold {
                        Some(min_threshold) if self.is_enabled => current_count > min_threshold,
                        _ => false,
                    }
                }
            }
            // if None, then we don't apply any back pressure.
            None => false,
        }
    }
}

pub async fn receive_new_message<M>(
    receiver: &mut M,
    is_back_pressure_enabled: bool,
) -> Option<Result<Option<ChannelMessage<MessengerCandidateMessage>>, MessageReceiverError>>
where
    M: MessageReciever<Message = ChannelMessage<MessengerCandidateMessage>> + Send + Sync + 'static,
{
    if is_back_pressure_enabled {
        // This sleep is fine, this just introduces a delay for fairness so that other async arms of tokio select can execute.
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
            // enable_back_pressure: false,
            in_progress_count: 0,
            back_pressure_configs,
        }
    }
    /// Get next versions with their commit actions to process.
    ///
    async fn process_next_actions(&mut self) -> MessengerServiceResult<u32> {
        let max_new_items_to_pick = self.back_pressure_configs.get_remaining_count(self.in_progress_count);

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
            self.in_progress_count += 1;
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
                self.in_progress_count -= 1;

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

    /// Compared the `max_in_progress_count` in config against `self.in_progress_count` to determine if back pressure should be enabled.
    /// This helps to limit processing the incoming messages and therefore enable a bound on the suffix.
    pub(crate) fn check_for_back_pressure(&mut self) {
        // // Check to see if back pressure should be enabled.
        // let should_enable = self
        //     .config
        //     .max_in_progress
        //     .map_or(false, |max_in_progress_count| self.in_progress_count >= max_in_progress_count);

        let should_enable = self.back_pressure_configs.should_enable_back_pressure(self.in_progress_count);
        if should_enable {
            // if back pressure was not enabled earlier, but will be enabled now, print a waning.
            if !self.back_pressure_configs.is_enabled {
                warn!(
                    "Applying back pressure as the total number of records currently being processed ({}) >= max in progress allowed ({:?})",
                    self.in_progress_count, self.back_pressure_configs.max_threshold
                );
            }
            // info!(
            //     "Back pressure - max_threshold = {:?} | min_threshold = {:?} | current in progress count = {} ",
            //     self.back_pressure_configs.max_threshold, self.back_pressure_configs.min_threshold, self.in_progress_count
            // );
        }
        self.back_pressure_configs.is_enabled = should_enable;
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
                    debug!("Not consuming new messages due to backpressure enabled. Current in progress count = {} | Total feedbacks available in the feedback channel = {} | total actions send in the actions channel = {}", self.in_progress_count ,self.rx_feedback_channel.max_capacity() - self.rx_feedback_channel.capacity(), self.tx_actions_channel.max_capacity() - self.tx_actions_channel.capacity());
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

        // Check if back pressure should be enabled.
        self.check_for_back_pressure();

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
