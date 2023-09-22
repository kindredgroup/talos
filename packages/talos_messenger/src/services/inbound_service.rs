// 1. Kafka - Get candidate message
//  a. Store inmemory.
// 2. Kafka - Get decision message.
//  a. Update the store.
// 3. Handle `On Commit` part of the message
//  a. Can there be anything other than publishing to kafka?
//  b. what if the topic doesnt exist?
//  c. Any validation required on what is being published?
//  d. Publish T(k) only if all prioir items are published or if safepoint of T(k) is published?
//  e. If there are multiple messages to be published, should they be done serially?:-
//      i. If to the same topic
//     ii. If to another topic
// 4. After a message was published:-
//  a. Mark that item as processed.
//  b. Prune the store if contiguous items are processed.

use ahash::{HashMap, HashMapExt};
use async_trait::async_trait;
use log::{error, info, warn};

use talos_certifier::{model::DecisionMessageTrait, ports::MessageReciever, ChannelMessage};
use talos_suffix::{Suffix, SuffixTrait};
use tokio::sync::mpsc;

use crate::{
    core::{MessengerChannelFeedback, MessengerCommitActions, MessengerSystemService},
    errors::{MessengerServiceError, MessengerServiceResult},
    suffix::{MessengerCandidate, MessengerSuffixItemTrait, MessengerSuffixTrait, SuffixItemCompleteStateReason, SuffixItemState},
    utlis::get_filtered_commit_actions,
};

pub struct MessengerInboundService<M>
where
    M: MessageReciever<Message = ChannelMessage> + Send + Sync + 'static,
{
    pub message_receiver: M,
    pub tx_actions_channel: mpsc::Sender<MessengerCommitActions>,
    pub rx_feedback_channel: mpsc::Receiver<MessengerChannelFeedback>,
    pub suffix: Suffix<MessengerCandidate>,
    pub allowed_actions: HashMap<&'static str, Vec<&'static str>>,
}

impl<M> MessengerInboundService<M>
where
    M: MessageReciever<Message = ChannelMessage> + Send + Sync + 'static,
{
    /// Get next items to process.
    async fn process_next_actions(&mut self) -> MessengerServiceResult {
        let items_to_process = self.suffix.get_suffix_items_to_process();

        // {

        error!(
            "Items to process count... {:#?}",
            items_to_process.iter().map(|x| x.version).collect::<Vec<u64>>()
        );
        // }

        for item in items_to_process {
            let ver = item.version;

            let commit_actions = HashMap::new();
            let payload_to_send = MessengerCommitActions {
                version: ver,
                commit_actions: item.actions.iter().fold(commit_actions, |mut acc, (key, value)| {
                    acc.insert(key.to_string(), value.payload.clone());
                    acc
                }),
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
                Ok(Some(msg)) = self.message_receiver.consume_message() => {
                    // if let Ok(Some(msg)) = res {

                        // 2. Add/update to suffix.
                        match msg {
                            // 2.1 For CM - Install messages on the version
                            ChannelMessage::Candidate(message) => {
                                let version = message.version;
                                if message.version > 0 {


                                    // insert item to suffix
                                    let _ = self.suffix.insert(version, message.into());

                                    if let Some(item_to_update) = self.suffix.get_mut(version){
                                        if let Some(commit_actions) = &item_to_update.item.candidate.on_commit {

                                            let filter_actions = get_filtered_commit_actions(commit_actions, &self.allowed_actions);

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
                                        error!("[FILTERED ACTIONS] version={}  state={:?} actions={:#?}", version, item_to_update.item.state, item_to_update.item.commit_actions);
                                    };

                                } else {
                                    warn!("Version 0 will not be inserted into suffix.")
                                }

                            },
                            // 2.2 For DM - Update the decision with outcome + safepoint.
                            ChannelMessage::Decision(decision_version, decision_message) => {
                                let version = decision_message.get_candidate_version();
                                info!("[Decision Message] Version received = {} and {}", decision_version, version);

                                self.suffix.update_item_decision(version, decision_version, &decision_message);

                                self.process_next_actions().await?


                                // TODO: GK - Calculate the safe offset to commit.

                                // TODO: GK - Prune suffix.

                            },
                        }

                    // }
                }
                // Next condition - Commit, get processed/published info.



                // Receive feedback from publisher.
                Some(feedback) = self.rx_feedback_channel.recv() => {
                    match feedback {
                        // TODO: GK - What to do when we have error on publishing? Retry??
                        MessengerChannelFeedback::Error(_, _) => panic!("Implement the error feedback"),
                        MessengerChannelFeedback::Success(version, key, total_count) => {
                            info!("Successfully received version={version} count={total_count}");

                            let  item_state = self.suffix.get_item_state(version);
                            match item_state{
                                Some(SuffixItemState::Processing) => {
                                    self.suffix.set_item_state(version, SuffixItemState::PartiallyComplete);

                                    // TODO: GK - Put setter function for this and avoid repeating it.
                                    if let Some(item_to_update) = self.suffix.get_mut(version) {
                                        if let Some(action_completed) = item_to_update.item.commit_actions.get_mut(&key) {
                                            action_completed.count += 1;
                                            action_completed.is_completed = action_completed.count  == total_count;

                                        }

                                        if item_to_update.item.commit_actions.iter().all(|(_,  x)| x.is_completed) {
                                                self.suffix.set_item_state(version, SuffixItemState::Complete(SuffixItemCompleteStateReason::Processed));

                                        }
                                    }

                                }
                                Some(SuffixItemState::PartiallyComplete) => {
                                    self.suffix.set_item_state(version, SuffixItemState::PartiallyComplete);

                                    // TODO: GK - Put setter function for this.
                                    if let Some(item_to_update) = self.suffix.get_mut(version) {
                                        if let Some(action_completed) = item_to_update.item.commit_actions.get_mut(&key) {
                                            action_completed.count += 1;

                                            if action_completed.count  == total_count {
                                                action_completed.is_completed = true;

                                            }
                                        }

                                        if item_to_update.item.commit_actions.iter().all(|(_, x)| x.is_completed) {
                                                self.suffix.set_item_state(version, SuffixItemState::Complete(SuffixItemCompleteStateReason::Processed));

                                        }
                                    }

                                },
                                _ =>(),
                            };

                            error!("State change for version={version} from {item_state:?} => {:?}", self.suffix.get_item_state(version));

                            // self.suffix.messages.iter().flatten().for_each(|item|
                            //     error!("version={} decision={:?} state={:?} action_state={:#?}", item.item_ver, item.item.decision, item.item.get_state(), item.item.commit_actions.iter().map(|x| (x.1.count, x.1.is_completed)).collect::<Vec<(u32, bool)>>())
                            // );
                            // info!("Suffix dump ={:?}")
                            // info!("State on completion ={:?}", item_state);
                        },
                    }
                    // Process the next items with commit actions
                    self.process_next_actions().await?

                }
            }
        }
    }
}
