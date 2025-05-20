use ahash::HashMap;
use async_trait::async_trait;
use strum::{Display, EnumString};
use talos_certifier::{
    errors::SystemServiceError,
    ports::{
        common::SharedPortTraits,
        errors::{MessagePublishError, MessageReceiverError},
        MessageReciever,
    },
    ChannelMessage,
};
use talos_suffix::{core::SuffixConfig, Suffix};
use tokio::{
    sync::mpsc::{self, Sender},
    task::JoinHandle,
};
use tracing::{debug, error};

use crate::{
    core::{ActionService, MessengerChannelFeedback, MessengerCommitActions, MessengerPublisher, PublishActionType},
    errors::{MessengerActionError, MessengerServiceResult},
    models::MessengerCandidateMessage,
    services::{MessengerInboundService, MessengerInboundServiceConfig},
    suffix::MessengerCandidate,
    utlis::get_actions_deserialised,
};

use super::payload::on_commit::MockOnCommitKafkaMessage;

// ################ Mock receiver - Start ##########################

/// Mock receiver to mimic Kafka consumer to use in tests.
pub struct MockReciever {
    pub consumer: mpsc::Receiver<ChannelMessage<MessengerCandidateMessage>>,
    pub offset: i64,
}

#[async_trait]
impl MessageReciever for MockReciever {
    type Message = ChannelMessage<MessengerCandidateMessage>;

    async fn consume_message(&mut self) -> Result<Option<Self::Message>, MessageReceiverError> {
        let msg = self.consumer.recv().await.unwrap();

        // let vers = match &msg {
        //     ChannelMessage::Candidate(msg) => Some(msg.message.version),
        //     ChannelMessage::Decision(decision) => Some(decision.decision_version),
        // };

        Ok(Some(msg))
    }

    async fn subscribe(&self) -> Result<(), SystemServiceError> {
        Ok(())
    }

    fn commit(&self) -> Result<(), Box<SystemServiceError>> {
        Ok(())
    }
    fn commit_async(&self) -> Option<JoinHandle<Result<(), SystemServiceError>>> {
        None
    }
    fn update_offset_to_commit(&mut self, version: i64) -> Result<(), Box<SystemServiceError>> {
        self.offset = version;
        Ok(())
    }
    async fn update_savepoint_async(&mut self, _version: i64) -> Result<(), SystemServiceError> {
        Ok(())
    }

    async fn unsubscribe(&self) -> () {}
}

#[async_trait]
impl SharedPortTraits for MockReciever {
    async fn is_healthy(&self) -> bool {
        true
    }
    async fn shutdown(&self) -> bool {
        false
    }
}

// ################ Mock receiver  - End ##########################

// ################ Mock Producer  - Start ##########################
pub struct MockProducer {
    pub tx_feedback_channel: Sender<MessengerChannelFeedback>,
}

impl MockProducer {
    pub async fn send_error_feedback(&self, version: u64) -> Result<(), MessagePublishError> {
        self.tx_feedback_channel
            .send(MessengerChannelFeedback::Error(
                version,
                PublishActionType::Kafka.to_string(),
                Box::new(MessengerActionError {
                    kind: crate::errors::MessengerActionErrorKind::Publishing,
                    reason: "Failed to publish".to_owned(),
                    data: version.to_string(),
                }),
            ))
            .await
            .unwrap();
        Ok(())
    }

    pub async fn send_success_feedback(&self, version: u64) -> Result<(), MessagePublishError> {
        self.tx_feedback_channel
            .send(MessengerChannelFeedback::Success(version, PublishActionType::Kafka.to_string()))
            .await
            .unwrap();
        Ok(())
    }

    pub async fn send_no_feedback(&self, _version: u64) -> Result<(), MessagePublishError> {
        Ok(())
    }
}

#[async_trait]
impl MessengerPublisher for MockProducer {
    type Payload = MockOnCommitKafkaMessage;

    type AdditionalData = i32;

    fn get_publish_type(&self) -> PublishActionType {
        PublishActionType::Kafka
    }

    async fn send(
        &self,
        version: u64,
        _payload: Self::Payload,
        _headers: HashMap<String, String>,
        _additional_data: Self::AdditionalData,
    ) -> Result<(), MessagePublishError> {
        self.tx_feedback_channel
            .send(MessengerChannelFeedback::Success(version, PublishActionType::Kafka.to_string()))
            .await
            .unwrap();
        Ok(())
    }
}

// ################ Mock Producer  - End ##########################

// ################ Mock Action Service  - Start ##########################

#[derive(EnumString, Display)]
pub enum FeedbackTypeHeader {
    #[strum(serialize = "success")]
    Success,
    #[strum(serialize = "error")]
    Error,
    #[strum(serialize = "partial")]
    Partial,
    #[strum(serialize = "no-feedback")]
    NoFeedback,
}

pub struct MockActionService {
    pub publisher: MockProducer,
    pub rx_actions_channel: mpsc::Receiver<MessengerCommitActions>,
    pub tx_feedback_channel: mpsc::Sender<MessengerChannelFeedback>,
}

#[async_trait]
impl ActionService for MockActionService {
    async fn process_action(&mut self) -> MessengerServiceResult {
        let actions_result = self.rx_actions_channel.try_recv();
        match actions_result {
            Ok(actions) => {
                let MessengerCommitActions {
                    version,
                    commit_actions,
                    headers,
                } = actions;

                error!("Headers... {headers:#?}");
                if let Some(publish_actions_for_type) = commit_actions.get(&self.publisher.get_publish_type().to_string()) {
                    match get_actions_deserialised::<Vec<MockOnCommitKafkaMessage>>(publish_actions_for_type) {
                        Ok(actions) => {
                            for _ in actions {
                                match headers.get("feedbackType").map(|f| f.to_owned()) {
                                    Some(feedback) if feedback == FeedbackTypeHeader::Error.to_string() => {
                                        self.publisher.send_error_feedback(version).await.unwrap();
                                    }
                                    Some(feedback) if feedback == FeedbackTypeHeader::Partial.to_string() => {
                                        self.publisher.send_success_feedback(version).await.unwrap();
                                        break;
                                    }
                                    Some(feedback) if feedback == FeedbackTypeHeader::NoFeedback.to_string() => {
                                        self.publisher.send_no_feedback(version).await.unwrap();
                                        break;
                                    }
                                    _ => {
                                        self.publisher.send_success_feedback(version).await.unwrap();
                                    }
                                };
                            }
                        }
                        Err(err) => {
                            error!(
                                "Failed to deserialise for version={version} key={} for data={:?} with error={:?}",
                                &self.publisher.get_publish_type(),
                                err.data,
                                err.reason
                            );
                        }
                    }
                }
            }
            _ => {
                debug!("No actions to process..")
            }
        }
        Ok(())
    }
}

// pub struct MockCandidateDecisionChannelMessage{
//     pub candidate: CandidateMessage
// }

//                                     | --- actions --> |
//  Abcast - cm/dm -> Inbound_service -|                 | - actions_service <-> publish
//                                     |                 |                       using Abcast
//                                     | <-- feedback -- |

pub struct MessengerServicesTesterConfigs {
    suffix_configs: SuffixConfig,
    inbound_service_configs: MessengerInboundServiceConfig,
}

impl MessengerServicesTesterConfigs {
    pub fn new(suffix_configs: SuffixConfig, inbound_service_configs: MessengerInboundServiceConfig) -> Self {
        MessengerServicesTesterConfigs {
            suffix_configs,
            inbound_service_configs,
        }
    }
}

pub struct MessengerServiceTester<A, M, T>
where
    A: ActionService,
    M: MessageReciever<Message = ChannelMessage<MessengerCandidateMessage>> + Send + Sync + 'static,
    T: Send + Sync,
{
    tx_channel_message: mpsc::Sender<T>,
    inbound_service: MessengerInboundService<M>,
    action_service: A,
}

impl MessengerServiceTester<MockActionService, MockReciever, ChannelMessage<MessengerCandidateMessage>> {
    pub fn new_with_mock_action_service(configs: MessengerServicesTesterConfigs) -> Self {
        let (tx_message_channel, rx_message_channel) = mpsc::channel(10);

        let message_receiver = MockReciever {
            consumer: rx_message_channel,
            offset: 0,
        };

        let (tx_actions_channel, rx_actions_channel) = mpsc::channel(10);
        let (tx_feedback_channel, rx_feedback_channel) = mpsc::channel(10);
        let suffix = Suffix::with_config(configs.suffix_configs, None);
        // let mut allowed_actions = AHashMap::new();
        // allowed_actions.insert("publish".to_owned(), vec!["kafka".to_owned()]);

        // let config = MessengerInboundServiceConfig::new(allowed_actions.into(), Some(10), Some(60 * 60 * 1_000));
        let messenger_service = MessengerInboundService::new(
            message_receiver,
            tx_actions_channel.clone(),
            rx_feedback_channel,
            suffix,
            configs.inbound_service_configs,
        );

        // START - Mock Action Service

        let mock_producer = MockProducer {
            tx_feedback_channel: tx_feedback_channel.clone(),
        };
        let mock_action_service = MockActionService {
            publisher: mock_producer,
            rx_actions_channel,
            tx_feedback_channel: tx_feedback_channel.clone(),
        };
        // END - Mock Action Service

        MessengerServiceTester {
            tx_channel_message: tx_message_channel,
            inbound_service: messenger_service,
            action_service: mock_action_service,
        }
    }

    /// Process the journey of `Candidate` and `Decision` through multiple services in the messenger process.
    ///
    /// Params are used to control which part of the journey to execute.
    /// - `candidate` - If candidate is Some(ChannelMessage), then run the abcast to inbound_service for candidate message.
    /// - `decision`  - If decision is Some(ChannelMessage), then run the abcast to inbound_service for decision message.
    /// - `journey_config.should_process_actions` - Controls if actions should be processed by the actions_service.
    /// - `journey_config.expected_feedbacks_in_journey` - This field controls how many iteration of the inbound_service must be run at last to receive the feedbacks from actions.
    pub async fn process_message_journey(
        &mut self,
        candidate: Option<ChannelMessage<MessengerCandidateMessage>>,
        decision: Option<ChannelMessage<MessengerCandidateMessage>>,
        journey_configs: Option<JourneyConfig>,
    ) {
        // Runs Abcast -> Inbound service journey for `Candidate`. This will flow through the `Candidate` message path of the service.
        if let Some(candidate) = candidate {
            self.publish_message(candidate).await;
            self.inbound_service.run_once().await.unwrap();
        }
        // Runs Abcast -> Inbound service journey for `Decision`. This will flow through the `Decision` message path of the service.
        if let Some(decision) = decision {
            self.publish_message(decision).await;
            self.inbound_service.run_once().await.unwrap();
        }

        if let Some(configs) = journey_configs {
            // Inbound Service -> Action Service.
            if configs.should_process_actions {
                self.action_service.process_action().await.unwrap();
            }

            // Action Service -> Inbound Service.
            if configs.expected_feedbacks_in_journey > 0 {
                for _ in 1..=configs.expected_feedbacks_in_journey {
                    self.inbound_service.run_once().await.unwrap();
                }
            }
        }
    }
}

pub struct JourneyConfig {
    /// Should the journey process the actions in the actions service.
    should_process_actions: bool,
    /// There can be `n` number of actions under `on_commit` of a version.
    /// Therefore this field helps to iterate through the feedbacks from actions_service for the actions.
    expected_feedbacks_in_journey: u32,
}

impl JourneyConfig {
    pub fn new(process_actions: bool, expected_feedbacks: u32) -> Self {
        JourneyConfig {
            expected_feedbacks_in_journey: expected_feedbacks,
            should_process_actions: process_actions,
        }
    }
}

impl<A, M, T> MessengerServiceTester<A, M, T>
where
    M: MessageReciever<Message = ChannelMessage<MessengerCandidateMessage>> + Send + Sync + 'static,
    A: ActionService,
    T: Send + Sync,
{
    pub fn get_suffix(&self) -> &Suffix<MessengerCandidate> {
        &self.inbound_service.suffix
    }

    async fn publish_message(&mut self, message: T) {
        self.tx_channel_message.send(message).await.unwrap();
    }
}
