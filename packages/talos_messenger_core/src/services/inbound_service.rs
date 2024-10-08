use ahash::{HashMap, HashMapExt};
use async_trait::async_trait;
use log::{debug, error, info, warn};

use talos_certifier::{model::DecisionMessageTrait, ports::MessageReciever, ChannelMessage};
use talos_suffix::{Suffix, SuffixTrait};
use tokio::sync::mpsc;

use crate::{
    core::{MessengerChannelFeedback, MessengerCommitActions, MessengerSystemService},
    errors::{MessengerServiceError, MessengerServiceResult},
    suffix::{MessengerCandidate, MessengerSuffixItemTrait, MessengerSuffixTrait, SuffixItemCompleteStateReason, SuffixItemState},
    utlis::get_allowed_commit_actions,
};

pub struct MessengerInboundService<M>
where
    M: MessageReciever<Message = ChannelMessage> + Send + Sync + 'static,
{
    pub message_receiver: M,
    pub tx_actions_channel: mpsc::Sender<MessengerCommitActions>,
    pub rx_feedback_channel: mpsc::Receiver<MessengerChannelFeedback>,
    pub suffix: Suffix<MessengerCandidate>,
    pub allowed_actions: HashMap<String, Vec<String>>,
}

impl<M> MessengerInboundService<M>
where
    M: MessageReciever<Message = ChannelMessage> + Send + Sync + 'static,
{
    /// Get next versions with their commit actions to process.
    ///
    async fn process_next_actions(&mut self) -> MessengerServiceResult {
        let items_to_process = self.suffix.get_suffix_items_to_process();

        for item in items_to_process {
            let ver = item.version;

            let payload_to_send = MessengerCommitActions {
                version: ver,
                commit_actions: item.actions.iter().fold(HashMap::new(), |mut acc, (key, value)| {
                    acc.insert(key.to_string(), value.get_payload().clone());
                    acc
                }),
                headers: item.headers,
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
        }

        Ok(())
    }

    /// Checks if all actions are completed and updates the state of the item to `Processed`.
    /// Also, checks if the suffix can be pruned and the message_receiver can be committed.
    pub(crate) fn check_and_update_all_actions_complete(&mut self, version: u64, reason: SuffixItemCompleteStateReason) {
        match self.suffix.are_all_actions_complete_for_version(version) {
            Ok(is_completed) if is_completed => {
                self.suffix.set_item_state(version, SuffixItemState::Complete(reason));

                //  Update the prune index in suffix if applicable.
                let prune_index = self.suffix.update_prune_index_from_version(version);

                // If there is a prune_index, it is safe to assume, all messages prioir to this are decided + on_commit actions are actioned.
                // Therefore, it is safe to commit till that offset/version.
                if let Some(index) = prune_index {
                    let prune_item_option = self.suffix.messages.get(index);

                    if let Some(Some(prune_item)) = prune_item_option {
                        let commit_offset = prune_item.item_ver + 1;
                        debug!("[Commit] Updating tpl to version .. {commit_offset}");
                        let _ = self.message_receiver.update_offset_to_commit(commit_offset as i64);

                        self.message_receiver.commit_async();
                    }
                }

                debug!("[Actions] All actions for version {version} completed!");
                // Check prune eligibility by looking at the prune meta info.
                if let Some(index_to_prune) = self.suffix.get_safe_prune_index() {
                    // Call prune method on suffix.
                    let _ = self.suffix.prune_till_index(index_to_prune);
                }
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
                self.suffix.set_item_state(version, SuffixItemState::PartiallyComplete);

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
}

#[async_trait]
impl<M> MessengerSystemService for MessengerInboundService<M>
where
    M: MessageReciever<Message = ChannelMessage> + Send + Sync + 'static,
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
            tokio::select! {
                // 1. Consume message.
                // Ok(Some(msg)) = self.message_receiver.consume_message() => {
                reciever_result = self.message_receiver.consume_message() => {

                    match reciever_result {
                        // 2.1 For CM - Install messages on the version
                        Ok(Some(ChannelMessage::Candidate(candidate))) => {
                            let version = candidate.message.version;
                            debug!("Candidate version received is {version}");
                            if version > 0 {
                                // insert item to suffix
                                let _ = self.suffix.insert(version, candidate.message.into());

                                if let Some(item_to_update) = self.suffix.get_mut(version){
                                    if let Some(commit_actions) = &item_to_update.item.candidate.on_commit {
                                        let filter_actions = get_allowed_commit_actions(commit_actions, &self.allowed_actions);
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
                            info!("[Decision Message] Version received = {} and {}", decision.decision_version, version);

                            // TODO: GK - no hardcoded filters on headers
                            let headers: HashMap<String, String> = decision.headers.into_iter().filter(|(key, _)| key.as_str() != "messageType").collect();
                            self.suffix.update_item_decision(version, decision.decision_version, &decision.message, headers);

                            self.process_next_actions().await?;

                        },
                        Ok(None) => {
                            info!("No message to process..");
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
                }
                // Receive feedback from publisher.
                feedback_result = self.rx_feedback_channel.recv() => {
                    match feedback_result {
                        Some(MessengerChannelFeedback::Error(version, key, message_error)) => {
                            error!("Failed to process version={version} with error={message_error:?}");
                            self.handle_action_failed(version, &key);

                        },
                        Some(MessengerChannelFeedback::Success(version, key)) => {
                            info!("Successfully processed version={version} with action_key={key}");
                            self.handle_action_success(version, &key);
                        },
                        None => {
                            debug!("No feedback message to process..");
                        }
                    }
                    // Process the next items with commit actions
                    self.process_next_actions().await?

                }
            }
        }
    }
}
