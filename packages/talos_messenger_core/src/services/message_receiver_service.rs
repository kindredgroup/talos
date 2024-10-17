use std::{
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
    time::Duration,
};

use ahash::{HashMap, HashMapExt};
use async_trait::async_trait;
use log::{debug, error, info, warn};

use talos_certifier::{model::DecisionMessageTrait, ports::MessageReciever, ChannelMessage};
use talos_suffix::{Suffix, SuffixTrait};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::sync::mpsc;

use crate::{
    core::{MessengerCommitActions, MessengerSystemService},
    errors::{MessengerServiceError, MessengerServiceResult},
    suffix::{
        MessengerCandidate, MessengerStateTransitionTimestamps, MessengerSuffixItemTrait, MessengerSuffixTrait, SuffixItemCompleteStateReason, SuffixItemState,
    },
    utlis::get_allowed_commit_actions,
};

pub struct MessengerInboundReceiverService<M>
where
    M: MessageReciever<Message = ChannelMessage> + Send + Sync + 'static,
{
    pub message_receiver: M,
    pub tx_actions_channel: mpsc::Sender<MessengerCommitActions>,
    pub suffix: Suffix<MessengerCandidate>,
    pub allowed_actions: HashMap<String, Vec<String>>,
    pub commit_offset: Arc<AtomicI64>,
}

impl<M> MessengerInboundReceiverService<M>
where
    M: MessageReciever<Message = ChannelMessage> + Send + Sync + 'static,
{
    /// Get next versions with their commit actions to process.
    ///
    async fn process_next_actions(&mut self) -> MessengerServiceResult {
        let items_to_process = self.suffix.get_suffix_items_to_process();

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

            info!("Payload being send... \n{payload_to_send:#?}");
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
}

#[async_trait]
impl<M> MessengerSystemService for MessengerInboundReceiverService<M>
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

        // TODO: GK - Duration should be configurable
        let mut interval = tokio::time::interval(Duration::from_millis(20));

        // let mut candidate_message_count = 0;
        // let mut decision_message_count = 0;
        // let mut on_commit_actions_feedback_count = 0;
        loop {
            tokio::select! {
                // biased;
                // 1. Consume message.
                // Ok(Some(msg)) = self.message_receiver.consume_message() => {
                reciever_result = self.message_receiver.consume_message() => {
                    // info!("[TOKIO::SELECT KAFKA CANDIDATE/DECISION ARM] - Receiving message from kafka");
                    match reciever_result {
                        // 2.1 For CM - Install messages on the version
                        Ok(Some(ChannelMessage::Candidate(candidate))) => {
                            // candidate_message_count += 1;
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

                                self.process_next_actions().await?;


                            } else {
                                warn!("Version 0 will not be inserted into suffix.")
                            }
                        },
                        // 2.2 For DM - Update the decision with outcome + safepoint.
                        Ok(Some(ChannelMessage::Decision(decision))) => {
                            // decision_message_count += 1;
                            let version = decision.message.get_candidate_version();
                            debug!("[Decision Message] Version received = {} and {}", decision.decision_version, version);

                            // TODO: GK - no hardcoded filters on headers
                            let headers: HashMap<String, String> = decision.headers.into_iter().filter(|(key, _)| key.as_str() != "messageType").collect();
                            self.suffix.update_item_decision(version, decision.decision_version, &decision.message, headers);

                            self.process_next_actions().await?;

                        },
                        Ok(None) => {
                            info!("No message to process..");
                            self.process_next_actions().await?;
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
                // Commit offset and prune suffix]
                _ = interval.tick() => {

                    let new_commit_offset = self.commit_offset.load(Ordering::Relaxed);
                    if new_commit_offset > 0 {
                        let version = new_commit_offset as u64;
                        // Issue commit
                        // TODO: GK - use proper error handling instead of unwrap here.
                        self.message_receiver.update_offset_to_commit(new_commit_offset).unwrap();
                        self.message_receiver.commit_async();

                        // Prune suffix
                        //  - Update the prune index if greater than current prune index
                        let index = self.suffix.index_from_head(version);
                        // TODO: GK - Is it okay to update the prune index blindly without any further checks?
                        self.suffix.update_prune_index(index);
                        // Check prune eligibility by looking at the prune meta info.
                        if let Some(index_to_prune) = self.suffix.get_safe_prune_index() {
                            // Call prune method on suffix.
                            let prev_suffix_length = self.suffix.suffix_length();

                            let _ = self.suffix.prune_till_index(index_to_prune);

                            let new_suffix_length = self.suffix.suffix_length();
                            let new_suffix_meta = self.suffix.get_meta();
                            // warn!(
                            //     "After pruning - before prune suffix lenght={prev_suffix_length}, new suffix length={new_suffix_length}, new prune_index={:?}, new head={}",
                            //     new_suffix_meta.prune_index, new_suffix_meta.head
                            // );
                        }
                    }

                    // Process the next set of actions
                    self.process_next_actions().await?;
                }

                // Receive feedback from publisher.
                // feedback_result = self.rx_feedback_channel.recv() => {

                //     let start_ms = Instant::now();
                //     // on_commit_actions_feedback_count += 1;
                //     // log::warn!("Counts.... candidate_message_count={candidate_message_count} | decision_message_count={decision_message_count} | on_commit_actions_feedback_count={on_commit_actions_feedback_count}");
                //     match feedback_result {
                //         Some(feedbacks) => {
                //             let total_batch = feedbacks.len();
                //             self.handle_batch_feedbacks(feedbacks);
                //             let elapsed_batch_feedback = start_ms.elapsed();
                //             info!("[TOKIO::SELECT BATCH FEEDBACK ARM] - Processed {total_batch} feedbacks in {}", elapsed_batch_feedback.as_seconds_f64() * 1_000_f64);

                //         },
                //         None => {
                //             debug!("No feedback message to process..");
                //         }
                //     }
                //     // Process the next items with commit actions
                //     self.process_next_actions().await?;
                //     // info!("Exiting feedback batch tokio::select arm in {}ms", start_ms.elapsed().as_seconds_f64() * 1_000_f64);


                // }
            }
        }
    }
}
