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
use serde_json::Value;
use talos_certifier::{
    model::{Decision, DecisionMessageTrait},
    ports::MessageReciever,
    ChannelMessage,
};
use talos_suffix::{core::SuffixConfig, Suffix, SuffixTrait};
use tokio::sync::mpsc;

use crate::{
    core::{MessengerChannelFeedback, MessengerCommitActions, MessengerSystemService},
    errors::MessengerServiceResult,
    models::commit_actions::publish::OnCommitActions,
    suffix::{MessengerCandidate, MessengerSuffixTrait, SuffixItemState},
};

pub struct MessengerInboundService<M>
where
    M: MessageReciever<Message = ChannelMessage> + Send + Sync + 'static,
{
    pub message_receiver_abcast: M,
    pub tx_actions_channel: mpsc::Sender<MessengerCommitActions>,
    pub rx_feedback_channel: mpsc::Receiver<MessengerChannelFeedback>,
    pub suffix: Suffix<MessengerCandidate>,
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
                res = self.message_receiver_abcast.consume_message() => {
                    if let Ok(Some(msg)) = res {

                        // 2. Add/update to suffix.
                        match msg {
                            // 2.1 For CM - Install messages on the version
                            ChannelMessage::Candidate(message) => {
                                let version = message.version;

                                if version > 0 {
                                    let _ = self.suffix.insert(version, message.into());
                                } else {
                                    warn!("Version 0 will not be inserted into suffix.")
                                }

                            },
                            // 2.2 For DM - Update the decision with outcome + safepoint.
                            ChannelMessage::Decision(decision_version, decision_message) => {
                                let version = decision_message.get_candidate_version();
                                info!("[Decision Message] Version received = {} and {}", decision_version, version);
                                let _ = self.suffix.update_decision_suffix_item(version, decision_version);
                                self.suffix.update_item_decision(version, &decision_message);

                                // If abort, mark the item as processed.
                                if decision_message.is_abort() {
                                    self.suffix.set_item_state(version, SuffixItemState::Complete(crate::suffix::SuffixItemCompleteStateReason::Aborted));
                                }

                                // Get items to process
                                //
                                // Take while safepoint of the on commit is already processed.

                                //  How to record if all the `on_commits` are completed?
                                //  1. Messenger should filter out the relavant ones it should process.
                                //  2. Messenger should keep track of individual on commits that it is relavant
                                //  3. When all relavant ones are done, mark the CM as `processed`
                                let  items_to_process = self.suffix.get_suffix_items_to_process();

                                // {

                                    info!("Items to process count... {:#?}", items_to_process.len());
                                // }

                                {
                                    for item in items_to_process {
                                        let ver = item.version;
                                        // send for publishing
                                        self.tx_actions_channel.send(item).await.unwrap();
                                        // Mark item as in process
                                        self.suffix.set_item_state(ver, SuffixItemState::Inflight)
                                    }
                                }
                            },
                        }

                    }
                }
                // Next condition - Commit, get processed/published info.



                // Receive feedback from publisher.
                Some(feedback) = self.rx_feedback_channel.recv() => {
                    match feedback {
                        MessengerChannelFeedback::Error(_, _) => panic!("Implement the error feedback"),
                        MessengerChannelFeedback::Success(version, _count) => {
                            self.suffix.set_item_state(version, SuffixItemState::Complete(crate::suffix::SuffixItemCompleteStateReason::Processed))
                        },
                    }

                }
            }
        }
    }
}

// pub async fn talos_messenger<M>(mut certifier_message_receiver: M)
// where
//     M: MessageReciever<Message = ChannelMessage> + Send + Sync + 'static,
// {
//     // TODO: GK - Suffix should be passed as a param
//     // let suffix_config = SuffixConfig {
//     //     capacity: config.suffix_capacity,2
//     //     prune_start_threshold: config.suffix_prune_threshold,
//     //     min_size_after_prune: config.suffix_minimum_size_on_prune,
//     // };

//     let suffix_config = SuffixConfig {
//         capacity: 400_000,
//         prune_start_threshold: Some(2_000),
//         min_size_after_prune: None,
//     };
//     let mut suffix: Suffix<MessengerCandidate> = Suffix::with_config(suffix_config);

//     let (_tx_feedback_channel, mut rx_feedback_channel) = mpsc::channel::<MessengerChannelFeedback>(10_000);
//     let (tx_actions_channel, mut rx_actions_channel) = mpsc::channel::<MessengerChannelFeedback>(10_000);

//     //  TODO: GK - White list should be build from env as config.
//     // let mut whitelist = HashMap::new();
//     // whitelist.insert("publish", vec!["kafka"]);

//     loop {
//         tokio::select! {
//                 // 1. Consume message.
//             res = certifier_message_receiver.consume_message() => {
//                 if let Ok(Some(msg)) = res {

//                     // 2. Add/update to suffix.
//                     match msg {
//                         // 2.1 For CM - Install messages on the version
//                         ChannelMessage::Candidate(message) => {
//                             let version = message.version;

//                             if version > 0 {
//                                 let _ = suffix.insert(version, message.into());
//                             } else {
//                                 warn!("Version 0 will not be inserted into suffix.")
//                             }

//                         },
//                         // 2.2 For DM - Update the decision with outcome + safepoint.
//                         ChannelMessage::Decision(decision_version, decision_message) => {
//                             let version = decision_message.get_candidate_version();
//                             error!("Version received = {} and {}", decision_version, version);
//                             let _ = suffix.update_decision_suffix_item(version, decision_version);
//                             suffix.update_item_decision(version, &decision_message);

//                             // If abort, mark the item as processed.
//                             if decision_message.is_abort() {
//                                 suffix.set_item_state(version, SuffixItemState::Complete(crate::suffix::SuffixItemCompleteStateReason::Aborted));
//                             }
//                         },
//                     }
//                     // Get items to process
//                     //
//                     // Take while safepoint of the on commit is already processed.

//                     //  How to record if all the `on_commits` are completed?
//                     //  1. Messenger should filter out the relavant ones it should process.
//                     //  2. Messenger should keep track of individual on commits that it is relavant
//                     //  3. When all relavant ones are done, mark the CM as `processed`
//                     let  items_to_process = suffix.get_suffix_items_to_process();

//                     // {

//                     //     error!("Items to process... {:?}", &items_to_process.iter().map(|x|x.item_ver).collect::<Vec<u64>>());
//                     // }

//                     {
//                         for item in items_to_process {
//                             // send for publishing
//                             tx_actions_channel.send()
//                             // Mark item as in process
//                             suffix.set_item_state(item.version, SuffixItemState::Inflight)
//                         }
//                     }

//                 }
//             }
//             // Next condition - Commit, get processed/published info.

//             // Receive feedback from publisher.
//             Some(feedback) = rx_feedback_channel.recv() => {
//                 match feedback {
//                     MessengerChannelFeedback::Error(_, _) => todo!(),
//                     MessengerChannelFeedback::Success(version, _count) => {
//                         suffix.set_item_state(version, SuffixItemState::Complete(crate::suffix::SuffixItemCompleteStateReason::Processed))
//                     },
//                 }

//             }
//         }
//     }
// }
