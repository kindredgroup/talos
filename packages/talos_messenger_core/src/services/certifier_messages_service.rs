use std::{
    sync::{atomic::AtomicI64, Arc},
    time::Instant,
};

use ahash::{HashMap, RandomState};
use async_trait::async_trait;
use indexmap::IndexMap;
use log::{debug, error, info, warn};
use talos_certifier::model::CandidateMessage;
use tokio::sync::mpsc;

use crate::{
    core::{MessengerChannelFeedback, MessengerCommitActions, MessengerSystemService},
    errors::{MessengerServiceError, MessengerServiceResult},
};

pub struct InboundMessageItem {
    pub payload: CandidateMessage,
    pub headers: HashMap<String, String>,
    pub is_decided: bool,
}

#[derive(Debug, PartialEq)]
pub enum ActionState {
    InProgress,
    Completed,
}

pub struct ActionStatusItem {
    /// Initially 0, later when feedback comes through, the total count is set from it.
    pub total_count: u32,
    pub success_count: u32,
    pub error_count: u32,
    pub state: ActionState,
}

impl ActionStatusItem {
    pub fn new() -> Self {
        Self {
            total_count: 0,
            success_count: 0,
            error_count: 0,
            state: ActionState::InProgress,
        }
    }

    pub fn set_total_count(&mut self, count: u32) {
        self.total_count = count;
    }

    pub fn update_state(&mut self, state: ActionState) {
        self.state = state;
    }

    pub fn is_completed(&self) -> bool {
        self.total_count == self.error_count + self.success_count
    }
}

pub struct MessengerFeedbackLoopService {
    pub rx_actions_channel: mpsc::Receiver<MessengerCommitActions>,
    pub tx_actions_channel_passahead: mpsc::Sender<MessengerCommitActions>,
    pub rx_feedback_channel: mpsc::Receiver<MessengerChannelFeedback>,
    pub commit_offset: Arc<AtomicI64>,
}

impl MessengerFeedbackLoopService {
    pub fn new(
        rx_actions_channel: mpsc::Receiver<MessengerCommitActions>,
        tx_actions_channel_passahead: mpsc::Sender<MessengerCommitActions>,
        rx_feedback_channel: mpsc::Receiver<MessengerChannelFeedback>,
        commit_offset: Arc<AtomicI64>,
    ) -> Self {
        Self {
            rx_actions_channel,
            rx_feedback_channel,
            tx_actions_channel_passahead,
            commit_offset,
        }
    }
}

#[async_trait]
impl MessengerSystemService for MessengerFeedbackLoopService {
    async fn start(&self) -> MessengerServiceResult {
        info!("Start Messenger service");
        Ok(())
    }

    async fn stop(&self) -> MessengerServiceResult {
        todo!()
    }

    async fn run(&mut self) -> MessengerServiceResult {
        let mut actions_processing_map: IndexMap<u64, ActionStatusItem, RandomState> = IndexMap::default();
        let mut total_processed: u32 = 0;

        loop {
            tokio::select! {
                // Actions receiver
                action_received = self.rx_actions_channel.recv() => {

                    if let Some(actions) = action_received {
                        let version = actions.version;
                        actions_processing_map.insert(version, ActionStatusItem::new());
                        self.tx_actions_channel_passahead.send(actions).await.map_err(|e| MessengerServiceError {
                            kind: crate::errors::MessengerServiceErrorKind::Channel,
                            reason: e.to_string(),
                            data: Some(version.to_string()),
                            service: "Certifier Message Service".to_string(),
                        })?;
                    };
                }
                // Feedback receiver
                feedback = self.rx_feedback_channel.recv() => {


                    let mut version_received: Option<u64> = None;
                    match feedback {
                        Some(MessengerChannelFeedback::Error(version, _key, message_error, total_count) )=> {
                            error!("Failed to process version={version} with error={message_error:?}");
                            if let Some(item_to_update) = actions_processing_map.get_mut(&version) {
                                version_received = Some(version);
                                item_to_update.set_total_count(total_count);
                                item_to_update.error_count += 1;

                                total_processed+=1;
                            }
                        },
                        Some(MessengerChannelFeedback::Success(version, key, total_count)) => {
                            debug!("Successfully processed version={version} with action_key={key}");
                            if let Some(item_to_update) = actions_processing_map.get_mut(&version) {
                                version_received = Some(version);
                                item_to_update.set_total_count(total_count);
                                item_to_update.success_count += 1;
                                // item_to_update.set_state(ActionState::Completed);

                                total_processed+=1;
                            }
                        },
                        None => {
                            debug!("No feedback to process..");
                        }


                    }
                    // Update the state if applicable.
                    if let Some(version) = version_received {
                        if let Some(item) = actions_processing_map.get_mut(&version) {
                            if item.is_completed() {
                                item.update_state(ActionState::Completed);
                            }
                        }
                    };

                    // Update commit version
                    // TODO: GK - This value in > check should be configurable.
                    if total_processed > 5_000 {
                        let last_completed = actions_processing_map.iter().filter(|(_,v)| {
                            v.state == ActionState::Completed
                        }).last();


                        if let Some((last_version,  _ )) = last_completed {
                            // Set the commit here.
                            // warn!("New commit offset will be set to {last_version:?}");
                            self.commit_offset.store(*last_version as i64, std::sync::atomic::Ordering::Relaxed);

                            // Prune till this version.
                            let start_ms = Instant::now();
                            if let Some(index) = actions_processing_map.get_index_of(last_version){
                                let p_len = actions_processing_map.len();
                                let r = actions_processing_map.drain(..=index);
                                let drop_len = r.len();
                                drop(r);
                                let n_len = actions_processing_map.len();

                                // warn!("Drained {} records in {}ms... previous length={} and new length={} ", drop_len, start_ms.elapsed().as_millis(), p_len, n_len);

                            }


                            // Reset total processed counter
                            total_processed = 0;
                        }


                    }

                }

            }
        }
        // Ok(())
    }
}

// MessengerCommitActions {
//     version: 4309,
//     commit_actions: {
//         "kafka": Array [
//             Object {
//                 "_typ": String("KafkaMessage"),
//                 "key": String("ksp:bet.1:7f64d835-d640-42f6-b7f2-f7cf10143945:0"),
//                 "topic": String("dev.test.messenger"),
//                 "value": Object {
//                     "channel": String("web"),
//                     "currency": String("AUD"),
//                     "customerId": String(""),
//                     "rn": String("ksp:bet.1:7f64d835-d640-42f6-b7f2-f7cf10143945:0"),
//                 },
//             },
//         ],
//     },
//     headers: {
//         "certXid": "ce054656-356b-420a-a826-d59c082708e7",
//         "certAgent": "HERMES_LOAD_TEST",
//         "certVersion": "4309",
//         "messengerCandidateReceivedTimestamp": "2024-10-17T02:59:41.101281Z",
//         "certSafepoint": "0",
//         "certTime": "2024-10-17T02:55:39.538536Z",
//         "messengerDecisionReceivedTimestamp": "2024-10-17T02:59:42.164517Z",
//         "producer": "HERMES_LOAD_TEST",
//         "candidatePublishTimestamp": "2024-10-17T02:55:39.167196Z",
//         "majorVersion": "1",
//         "decisionPublishTimestamp": "2024-10-17T02:55:39.538537Z",
//         "messengerStartOnCommitActionsTimestamp": "2024-10-17T02:59:42.164562Z",
//         "messageEncoding": "application/json",
//         "decisionCreatedTimestamp": "2024-10-17T02:55:39.538536Z",
//         "candidateCreatedTimestamp": "2024-10-17T02:55:39.167183Z",
//     },
// }
