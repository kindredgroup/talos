// - Test updating decision in suffix.
//  -  Duplicate decisions
// - Test `prepare_offset_for_commit`

use ahash::{HashMap, HashMapExt};
use async_trait::async_trait;
use serde_json::{json, Value};
use talos_certifier::{
    errors::SystemServiceError,
    ports::{common::SharedPortTraits, errors::MessageReceiverError, MessageReciever},
    test_helpers::mocks::payload::{build_mock_outcome, MockCandidatePayload, MockChannelMessage},
    ChannelMessage,
};
use talos_suffix::{core::SuffixConfig, Suffix};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    core::Replicator,
    models::{ReplicatorCandidate, ReplicatorCandidateMessage},
    services::replicator_service::{ReplicatorService, ReplicatorServiceConfig},
    StatemapItem,
};

/// Mock receiver to mimic Kafka consumer to use in tests.
pub struct MockReciever {
    pub consumer: mpsc::Receiver<ChannelMessage<ReplicatorCandidateMessage>>,
    pub offset: i64,
}

#[async_trait]
impl MessageReciever for MockReciever {
    type Message = ChannelMessage<ReplicatorCandidateMessage>;

    async fn consume_message(&mut self) -> Result<Option<Self::Message>, MessageReceiverError> {
        let msg = self.consumer.recv().await.unwrap();

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

//
struct MockReplicatorChannelMessage {}

impl MockReplicatorChannelMessage {
    fn build_channel_message(
        version: u64,
        suffix_head: u64,
        safepoint: Option<u64>,
        conflict_version: Option<u64>,
        headers: &HashMap<String, String>,
        statemaps: Option<Vec<(String, Value)>>,
    ) -> (ChannelMessage<ReplicatorCandidateMessage>, ChannelMessage<ReplicatorCandidateMessage>) {
        let outcome = build_mock_outcome(conflict_version, safepoint);

        let cm_1_statemaps = build_mock_statemaps(version, safepoint, statemaps.unwrap_or(vec![default_statemap_item(), default_statemap_item()]));
        let cm_1: ReplicatorCandidateMessage = MockCandidatePayload::new().add_statemap(&serde_json::to_value(cm_1_statemaps).unwrap()).build();

        let cm_1_channel_msg = MockChannelMessage::new(&cm_1, version);
        let c_cm_1_channel_msg = cm_1_channel_msg.build_candidate_channel_message(headers);
        let d_cm_1_channel_msg = cm_1_channel_msg.build_decision_channel_message(version, &outcome, suffix_head, headers);

        (c_cm_1_channel_msg, d_cm_1_channel_msg)
    }
}

fn default_statemap_item() -> (String, Value) {
    ("DefaultAction".to_owned(), json!({"some":"payload"}))
}

fn build_mock_statemaps(version: u64, safepoint: Option<u64>, statemap_item_fns: Vec<(String, Value)>) -> Vec<StatemapItem> {
    let mut statemaps = vec![];

    for item in statemap_item_fns {
        statemaps.push(StatemapItem::new(item.0, version, item.1, safepoint));
    }

    statemaps
}

// - Test inserting candidates to suffix
//      - When candidate version is below suffix head
//      - When candidate version > suffix head
#[tokio::test]
async fn test_replicator_service_inserting_candidates_to_suffix() {
    let (statemaps_tx, _) = mpsc::channel(50);
    let (replicator_feedback_tx, replicator_feedback_rx) = mpsc::channel(50);
    let (tx_message_channel, rx_message_channel) = mpsc::channel(10);

    let receiver = MockReciever {
        consumer: rx_message_channel,
        offset: 0,
    };

    let config = ReplicatorServiceConfig {
        commit_frequency_ms: 60_000, // 1 hour
        enable_stats: false,
    };

    let suffix_config = SuffixConfig {
        capacity: 1_000,
        prune_start_threshold: Some(10),
        min_size_after_prune: None,
    };
    let suffix: Suffix<ReplicatorCandidate> = Suffix::with_config(suffix_config);

    let replicator = Replicator::new(receiver, suffix, None);

    let mut replicator_svc = ReplicatorService::new(statemaps_tx, replicator_feedback_rx, replicator, config);

    // ************* start of assertions *************** //
    let headers = HashMap::new();

    //******************************************************************************************************
    //*** Test when sending a version > suffix length while suffix is empty
    //******************************************************************************************************
    replicator_feedback_tx
        .send(crate::core::ReplicatorChannel::LastInstalledVersion(0))
        .await
        .unwrap();

    // Run the replicator service once to last installed version.
    replicator_svc.run_once().await.unwrap();

    assert_eq!(replicator_svc.replicator.suffix.meta.prune_index, None);
    assert_eq!(replicator_svc.replicator.suffix.messages.len(), 0);

    // ** candidate message with version 10.
    let vers_10 = 10;
    let (c_10, _) = MockReplicatorChannelMessage::build_channel_message(vers_10, replicator_svc.replicator.suffix.meta.head, Some(0), None, &headers, None);

    // Send the candidate message for version 10.
    tx_message_channel.send(c_10).await.unwrap();
    assert_eq!(tx_message_channel.max_capacity() - tx_message_channel.capacity(), 1);

    // Run the replicator service to process the candidate for version 10.
    replicator_svc.run_once().await.unwrap();
    // replicator_svc.run_once().await.unwrap();

    assert_eq!(replicator_svc.replicator.suffix.messages.len(), 1);

    // ** candidate message with version 6.
    let vers_6 = 6;
    let (c_6, _) = MockReplicatorChannelMessage::build_channel_message(vers_6, replicator_svc.replicator.suffix.meta.head, Some(0), None, &headers, None);

    // Send the candidate message for version 6.
    tx_message_channel.send(c_6).await.unwrap();
    // Run the replicator service to process the candidate for version 6.
    replicator_svc.run_once().await.unwrap();

    // Candidate version 6 will not be inserted as it is behind the head.
    assert_eq!(replicator_svc.replicator.suffix.meta.head, 10);
    assert_eq!(replicator_svc.replicator.suffix.messages.len(), 1);

    // ** candidate message with version 17.
    let vers_17 = 17;
    let (c_17, _) = MockReplicatorChannelMessage::build_channel_message(vers_17, replicator_svc.replicator.suffix.meta.head, Some(0), None, &headers, None);

    // Send the candidate message for version 17.
    tx_message_channel.send(c_17).await.unwrap();
    // Run the replicator service to process the candidate for version 1.
    replicator_svc.run_once().await.unwrap();

    // Candidate version 17 will be inserted.
    assert_eq!(replicator_svc.replicator.suffix.meta.head, 10);
    assert_eq!(replicator_svc.replicator.suffix.messages.len(), 8);
}

// - Test pruning.
//      - When version received is < head -> no pruning.
//      - When version received is == head -> pruning till head.
//      - When version received is > the suffix length -> pruning the whole suffix.
//      - When version received is > head and < suffix length -> pruning till the provided index.
// - Test items send for install.
//      - Check if we are sending duplicates for install
//      - Check we take only till the items that are decided
//      - Check if we are picking only the ones that are really required.
#[tokio::test]
async fn test_replicator_service_pruning_suffix() {
    let (statemaps_tx, statemaps_rx) = mpsc::channel(50);
    let (replicator_feedback_tx, replicator_feedback_rx) = mpsc::channel(50);
    let (tx_message_channel, rx_message_channel) = mpsc::channel(10);

    let receiver = MockReciever {
        consumer: rx_message_channel,
        offset: 0,
    };

    let config = ReplicatorServiceConfig {
        commit_frequency_ms: 60_000, // 1 hour
        enable_stats: false,
    };

    let suffix_config = SuffixConfig {
        capacity: 1_000,
        prune_start_threshold: Some(10),
        min_size_after_prune: None,
    };
    let suffix: Suffix<ReplicatorCandidate> = Suffix::with_config(suffix_config);

    let replicator = Replicator::new(receiver, suffix, None);

    let mut replicator_svc = ReplicatorService::new(statemaps_tx, replicator_feedback_rx, replicator, config);

    // ************* start of assertions *************** //

    assert_eq!(replicator_svc.replicator.suffix.meta.prune_index, None);
    assert_eq!(replicator_svc.replicator.suffix.meta.head, 0);

    //******************************************************************************************************
    //*** Test when sending a version > suffix length while suffix is empty
    //******************************************************************************************************
    replicator_feedback_tx
        .send(crate::core::ReplicatorChannel::LastInstalledVersion(20))
        .await
        .unwrap();

    // Run the replicator service once to last installed version.
    replicator_svc.run_once().await.unwrap();

    assert_eq!(replicator_svc.replicator.suffix.meta.prune_index, None);
    assert_eq!(replicator_svc.replicator.suffix.messages.len(), 0);

    let headers = HashMap::new();

    // ** candidate message with version 1.
    let vers_1 = 1;
    let (c_1, d_1) = MockReplicatorChannelMessage::build_channel_message(vers_1, replicator_svc.replicator.suffix.meta.head, Some(0), None, &headers, None);

    // Send the candidate message for version 1.
    tx_message_channel.send(c_1).await.unwrap();
    // Send the decision message for version 1.
    tx_message_channel.send(d_1).await.unwrap();
    // Run the replicator service to process the candidate for version 1.
    replicator_svc.run_once().await.unwrap();

    assert_eq!(replicator_svc.replicator.suffix.messages.len(), 1);
    // No items send for for install as decision is not processed by replicator service.
    assert_eq!(statemaps_rx.max_capacity() - statemaps_rx.capacity(), 0);

    // Run the replicator service to process the decision for version 1.
    replicator_svc.run_once().await.unwrap();
    assert_eq!(statemaps_rx.max_capacity() - statemaps_rx.capacity(), 1);

    // ** candidate message with version 4.
    let vers_4 = 4;
    let (c_4, d_4) = MockReplicatorChannelMessage::build_channel_message(vers_4, replicator_svc.replicator.suffix.meta.head, Some(0), None, &headers, None);

    // Send the candidate message for version 4.
    tx_message_channel.send(c_4).await.unwrap();
    // Send the decision message for version 4.
    tx_message_channel.send(d_4).await.unwrap();
    // Run the replicator service to process the candidate for version 4.
    replicator_svc.run_once().await.unwrap();

    assert_eq!(replicator_svc.replicator.suffix.messages.len(), 4);
    // No items send for for install as decision is not processed by replicator service.
    assert_eq!(statemaps_rx.max_capacity() - statemaps_rx.capacity(), 1);

    // Run the replicator service to process the decision for version 4.
    replicator_svc.run_once().await.unwrap();
    assert_eq!(statemaps_rx.max_capacity() - statemaps_rx.capacity(), 2);

    //******************************************************************************************************
    //*** Test when sending a version > suffix length - Prunes everything.
    //******************************************************************************************************
    replicator_feedback_tx
        .send(crate::core::ReplicatorChannel::LastInstalledVersion(20))
        .await
        .unwrap();

    // Run the replicator service once to prune based on last installed version.
    replicator_svc.run_once().await.unwrap();

    // Pruning doesn't happen as the item is not in suffix
    assert_eq!(replicator_svc.replicator.suffix.meta.head, 0);
    assert_eq!(replicator_svc.replicator.suffix.messages.len(), 0);

    // ** candidate message with version 6 with abort decision
    let vers_6 = 6;
    let (c_6, d_6) = MockReplicatorChannelMessage::build_channel_message(vers_6, replicator_svc.replicator.suffix.meta.head, None, None, &headers, None);

    // ** candidate message with version 12 with commit decision and safepoint 6
    let vers_12 = 12;
    let (c_12, d_12) = MockReplicatorChannelMessage::build_channel_message(vers_12, replicator_svc.replicator.suffix.meta.head, None, None, &headers, None);

    // Send the candidate message for version 6.
    tx_message_channel.send(c_6).await.unwrap();
    // Send the candidate message for version 12.
    tx_message_channel.send(c_12).await.unwrap();

    // Run the replicator service to process the candidate for version 6 and 12.
    replicator_svc.run_once().await.unwrap();
    replicator_svc.run_once().await.unwrap();

    // Send the decision message for version 12.
    tx_message_channel.send(d_12).await.unwrap();
    // Run the replicator service to process the decision for version 12.
    replicator_svc.run_once().await.unwrap();
    // Although version 12 is decided and ready to send, since version 6 hasn't been decided, the channel will have statemaps to install only for version 1 and 4.
    assert_eq!(replicator_svc.replicator.suffix.meta.head, 6);
    assert_eq!(statemaps_rx.max_capacity() - statemaps_rx.capacity(), 2);
    assert_eq!(replicator_svc.replicator.suffix.messages.len(), 7);
    assert_eq!(replicator_svc.replicator.last_installing, 4);

    // Send the decision message for version 12.
    tx_message_channel.send(d_6).await.unwrap();
    // Run the replicator service to process the decision for version 6.
    replicator_svc.run_once().await.unwrap();
    assert_eq!(replicator_svc.replicator.last_installing, 12);
    assert_eq!(statemaps_rx.max_capacity() - statemaps_rx.capacity(), 4);

    //***************************************************************************************
    //*** Test when sending a version which is below the head - Pruning will not happen
    //***************************************************************************************
    replicator_feedback_tx
        .send(crate::core::ReplicatorChannel::LastInstalledVersion(3))
        .await
        .unwrap();

    // Run the replicator service once to last installed version.
    replicator_svc.run_once().await.unwrap();

    assert_eq!(replicator_svc.replicator.suffix.meta.head, 6);
    assert_eq!(statemaps_rx.max_capacity() - statemaps_rx.capacity(), 4);
    assert_eq!(replicator_svc.replicator.suffix.messages.len(), 7);

    //***************************************************************************************
    //*** Test when sending a version which is equal to the head - Pruning will happen
    //***************************************************************************************
    replicator_feedback_tx
        .send(crate::core::ReplicatorChannel::LastInstalledVersion(6))
        .await
        .unwrap();

    // Run the replicator service once to last installed version.
    replicator_svc.run_once().await.unwrap();

    assert_eq!(statemaps_rx.max_capacity() - statemaps_rx.capacity(), 4);
    assert_eq!(replicator_svc.replicator.suffix.meta.head, 12);
    assert_eq!(replicator_svc.replicator.suffix.messages.len(), 1);

    // ** candidate message with version 15 with abort decision
    let vers_15 = 15;
    let (c_15, _d_15) = MockReplicatorChannelMessage::build_channel_message(vers_15, replicator_svc.replicator.suffix.meta.head, None, None, &headers, None);

    // ** candidate message with version 22 with commit decision and safepoint 6
    let vers_22 = 22;
    let (c_22, _d_22) = MockReplicatorChannelMessage::build_channel_message(vers_22, replicator_svc.replicator.suffix.meta.head, None, None, &headers, None);

    // // Send the candidate versions 15 adn 22.
    tx_message_channel.send(c_15).await.unwrap();
    tx_message_channel.send(c_22).await.unwrap();

    // Run the replicator service to process the candidates for version 15 and 22.
    replicator_svc.run_once().await.unwrap();
    replicator_svc.run_once().await.unwrap();

    assert_eq!(replicator_svc.replicator.suffix.meta.head, 12);
    assert_eq!(replicator_svc.replicator.suffix.messages.len(), 11);

    //*****************************************************************************************************************************
    //*** Test when sending an version which is between the head and suffix length - Pruning will happen
    //*****************************************************************************************************************************

    replicator_feedback_tx
        .send(crate::core::ReplicatorChannel::LastInstalledVersion(13))
        .await
        .unwrap();

    // Run the replicator service once to last installed version.
    replicator_svc.run_once().await.unwrap();

    // Pruning will not happen, as it is considered as invalid.
    assert_eq!(replicator_svc.replicator.suffix.meta.head, 15);
    assert_eq!(replicator_svc.replicator.suffix.messages.len(), 8);
}
