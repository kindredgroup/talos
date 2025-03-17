use ahash::{AHashMap, HashMap, HashMapExt};
use talos_certifier::model::CandidateMessage;
use talos_suffix::core::SuffixConfig;

use crate::{
    services::MessengerInboundServiceConfig,
    suffix::{MessengerSuffixAssertionTrait, SuffixItemState},
    tests::{
        payload::{
            candidate::{CandidateTestPayload, MockChannelMessage},
            on_commit::{build_kafka_on_commit_message, build_on_commit_publish_kafka_payload, get_default_kafka_payload, MockOnCommitMessage},
        },
        test_utils::{build_mock_outcome, FeedbackTypeHeader, JourneyConfig, MessengerServiceTester, MessengerServicesTesterConfigs},
    },
};

/// This test doesn't look at the feedbacks and asserts the below scenarios:
///  - Version 0 is ignored and has no effect on suffix `head` and `prune_index`
///  - Next version of candidate will be set as the `head`. Stored at index 0.
///  - When decision is `abort` for this version, safe to update the prune version till this version. And therefore `prune_index` will be updated
///  - Next version of candidate is inserted and state is `AwaitingDecision`.
///  - When decision is `commit`, the `prune_index` will not move ahead, as it is reliant on the feedback to move to `Complete` state.
///  - Without feedback, the state would be `Processing`.
#[tokio::test]
async fn test_suffix_without_feedback() {
    // START - Prep before test
    let mut allowed_actions = AHashMap::new();
    allowed_actions.insert("publish".to_owned(), vec!["kafka".to_owned()]);

    let inbound_service_configs = MessengerInboundServiceConfig::new(allowed_actions.into(), Some(10), Some(60 * 60 * 1_000));
    let configs = MessengerServicesTesterConfigs::new(
        SuffixConfig {
            capacity: 50,
            prune_start_threshold: Some(55),
            min_size_after_prune: None,
        },
        inbound_service_configs,
    );
    let mut service_tester = MessengerServiceTester::new_with_mock_action_service(configs);
    // END - Prep before test

    let mut kafka_vec = vec![];
    for _ in 0..1 {
        kafka_vec.push(build_kafka_on_commit_message("some-topic", "1234", None, None));
    }

    let on_commit = build_on_commit_publish_kafka_payload(kafka_vec);
    let candidate: CandidateMessage = CandidateTestPayload::new().add_on_commit(&on_commit).build();
    let headers = HashMap::new();

    let abort_outcome = build_mock_outcome(None, None);

    //  Send candidate with version = 0
    let cm_0 = MockChannelMessage::new(&candidate, 0);
    let cm_channel_msg_0 = cm_0.build_candidate_channel_message(&headers);

    service_tester.process_message_journey(Some(cm_channel_msg_0), None, None).await;
    let suffix = service_tester.get_suffix();
    assert_eq!(suffix.messages.len(), 0);

    //  Send candidate with version = 2
    let cm_2 = MockChannelMessage::new(&candidate, 2);
    let cm_channel_msg_2 = cm_2.build_candidate_channel_message(&headers);

    service_tester.process_message_journey(Some(cm_channel_msg_2), None, None).await;
    let suffix = service_tester.get_suffix();
    assert_eq!(suffix.messages.len(), 1);
    assert!(suffix.meta.prune_index.is_none());

    // Send decision for version = 0
    let dm_channel_msg_0 = cm_0.build_decision_channel_message(3, &abort_outcome, 0, &headers);
    service_tester.process_message_journey(None, Some(dm_channel_msg_0), None).await;
    let suffix = service_tester.get_suffix();
    assert_eq!(suffix.messages.len(), 1);
    assert!(suffix.meta.prune_index.is_none());

    // Send decision for version = 2
    // When decision for cm_2 (version =2) arrives with abort outcome, the state of suffix should be as follows
    // prune_index = Some(0)
    // head = 2
    let dm_channel_msg_2 = cm_2.build_decision_channel_message(5, &abort_outcome, 0, &headers);
    service_tester.process_message_journey(None, Some(dm_channel_msg_2), None).await;
    let suffix = service_tester.get_suffix();
    assert_eq!(suffix.messages.len(), 1);
    assert_eq!(suffix.meta.head, 2);
    assert!(suffix.meta.prune_index.is_some());
    assert_eq!(suffix.meta.prune_index, Some(0));

    //  Send candidate with version = 6
    let cm_6 = MockChannelMessage::new(&candidate, 6);
    let cm_channel_msg_6 = cm_6.build_candidate_channel_message(&headers);
    service_tester.process_message_journey(Some(cm_channel_msg_6), None, None).await;
    let suffix = service_tester.get_suffix();
    // Messenger suffix length will be 5 as head is 2.
    assert_eq!(suffix.messages.len(), 5);

    // cm_6 with commit decision
    let commit_outcome = build_mock_outcome(None, Some(0));
    let dm_channel_msg_6 = cm_6.build_decision_channel_message(7, &commit_outcome, 0, &headers);

    let suffix = service_tester.get_suffix();
    suffix.assert_item_state(6, &SuffixItemState::AwaitingDecision);

    service_tester.process_message_journey(None, Some(dm_channel_msg_6), None).await;
    let suffix = service_tester.get_suffix();
    assert_eq!(suffix.messages.len(), 5);
    assert_eq!(suffix.meta.prune_index, Some(0));
    // Assert the states of version 2 and version 6
    suffix.assert_item_state(2, &SuffixItemState::Complete(crate::suffix::SuffixItemCompleteStateReason::Aborted));
    suffix.assert_item_state(6, &SuffixItemState::Processing);
}

// These test checks the various states suffix items in messenger can be at.

// ############################################################# START -  state transition related tests  ############################################################# //
//
// We will run a series of tests below following the below test plan to see how different states are transitioned.
//      1. Test moving to final state based on different commit_action flavours.
//          - Relavant final states possible - `Complete(NoCommitActions)`, `Complete(NoRelavantCommitActions)`
//      2. Test moving to final state based on certification decision outcome.
//          - When abort decision - `Complete(Aborted)`
//      3. Test moving to final state based on feedback loop.
//          - When success feedback and safepoint item not on suffix - `Complete(Processed)`
//          - When success feedback and safepoint item is on suffix and either in `Processing`, `PartiallyComplete` or some `Complete(..)` state - `Complete(Processed)`
//          - When success feedback and safepoint item is on suffix and not in `Processing`, `PartiallyComplete` nor in some `Complete(..)` state - `ReadyToProcess`
//          - When error feedback - `Complete(ErrorProcessing)`
//      4. Test when a candidate is awaiting decision and candidate version is decided but has safepoint on first candidate
//          - First candidate state - `AwaitingDecision`
//          - Second candidate state - `ReadyToProcess`
//

#[tokio::test]
async fn test_suffix_item_state_by_on_commit() {
    // START - Prep before test
    let mut allowed_actions = AHashMap::new();
    allowed_actions.insert("publish".to_owned(), vec!["kafka".to_owned()]);

    let inbound_service_configs = MessengerInboundServiceConfig::new(allowed_actions.into(), Some(10), Some(60 * 60 * 1_000));
    let configs = MessengerServicesTesterConfigs::new(
        SuffixConfig {
            capacity: 50,
            prune_start_threshold: Some(55),
            min_size_after_prune: None,
        },
        inbound_service_configs,
    );
    let mut service_tester = MessengerServiceTester::new_with_mock_action_service(configs);
    // END - Prep before test

    // START - Prepare basic candidates with various different types of on-commit actions
    // Candidate with no on-commit action
    let candidate_with_no_on_commit: CandidateMessage = CandidateTestPayload::new().build();
    assert!(candidate_with_no_on_commit.on_commit.is_none());

    // Candidate with no supported on-commit action
    let on_commit = MockOnCommitMessage::build_from_str(r#"{"notSuportedAction": {"name": "Kindred"}}"#);
    let on_commit_value = on_commit.as_value();

    let candidate_with_irrelevant_on_commit: CandidateMessage = CandidateTestPayload::new().add_on_commit(&on_commit_value).build();
    assert!(candidate_with_irrelevant_on_commit.on_commit.is_some());

    // Candidate with on commit publish to kafka messages.
    let mut on_commit = MockOnCommitMessage::new();

    for _ in 0..5 {
        on_commit.insert_kafka_message("some-topic".to_owned(), None, None, get_default_kafka_payload());
    }
    let on_commit_value = on_commit.as_value();
    let candidate_with_on_commit: CandidateMessage = CandidateTestPayload::new().add_on_commit(&on_commit_value).build();
    assert!(candidate_with_on_commit.on_commit.is_some());
    // env_logger::init();
    // error!("candidate_with_on_commit is \n {candidate_with_on_commit:#?}");
    // END - Prepare basic candidates with various different types of on-commit actions.

    let headers = HashMap::new();

    // ########### Send Candidate version = 9 - no commit actions ########### //
    let cm_9 = MockChannelMessage::new(&candidate_with_no_on_commit, 9);
    let cm_channel_msg_9 = cm_9.build_candidate_channel_message(&headers);

    service_tester.process_message_journey(Some(cm_channel_msg_9), None, None).await;
    // tx_message_channel.send(cm_channel_msg_9).await.unwrap();
    // messenger_service.run_internal().await.unwrap();

    // Because version 9 has no `on_commit` actions, it will be moved directly to `Complete(NoCommitActions)`.
    // ~~~~~ State Transition:  ⮕ `Complete(NoCommitActions)` ✅  ~~~~~ //
    let suffix = service_tester.get_suffix();
    suffix.assert_item_state(9, &SuffixItemState::Complete(crate::suffix::SuffixItemCompleteStateReason::NoCommitActions));
    suffix.assert_suffix_head_and_prune_index(9, None);
    assert_eq!(suffix.suffix_length(), 1);

    // ########### Send Candidate version = 10 - no relavant commit actions ########### //
    let cm_10 = MockChannelMessage::new(&candidate_with_irrelevant_on_commit, 10);
    let cm_channel_msg_10 = cm_10.build_candidate_channel_message(&headers);

    // tx_message_channel.send(cm_channel_msg_10).await.unwrap();
    // messenger_service.run_internal().await.unwrap();
    service_tester.process_message_journey(Some(cm_channel_msg_10), None, None).await;

    // Because version 10 has no relavant `on_commit` actions, it will be moved directly to `Complete(NoRelavantCommitActions)`.
    // ~~~~~ State Transition:  ⮕ `Complete(NoRelavantCommitActions)` ✅  ~~~~~ //
    let suffix = service_tester.get_suffix();
    suffix.assert_item_state(
        10,
        &SuffixItemState::Complete(crate::suffix::SuffixItemCompleteStateReason::NoRelavantCommitActions),
    );

    // Because suffix has only version 9 and 10, there are only two items in suffix and since both are completed
    // prune_index should pointing to version 10's index i.e Some(1).
    // But prune_index logic runs only in decision path when the decision is abort, or in feedback path or in the interval path.
    suffix.assert_suffix_head_and_prune_index(9, None);
    assert_eq!(suffix.suffix_length(), 2);

    let abort_outcome = build_mock_outcome(None, None);
    let dm_channel_msg_9 = cm_9.build_decision_channel_message(11, &abort_outcome, 0, &headers);
    let dm_channel_msg_10 = cm_10.build_decision_channel_message(13, &abort_outcome, 0, &headers);

    service_tester.process_message_journey(None, Some(dm_channel_msg_9), None).await;
    service_tester.process_message_journey(None, Some(dm_channel_msg_10), None).await;

    // tx_message_channel.send(dm_channel_msg_9).await.unwrap();
    // tx_message_channel.send(dm_channel_msg_10).await.unwrap();
    // messenger_service.run_internal().await.unwrap();
    // messenger_service.run_internal().await.unwrap();

    let suffix = service_tester.get_suffix();
    suffix.assert_suffix_head_and_prune_index(9, Some(1));
}
#[tokio::test]
async fn test_suffix_item_state_by_decision() {
    // START - Prep before test
    let mut allowed_actions = AHashMap::new();
    allowed_actions.insert("publish".to_owned(), vec!["kafka".to_owned()]);

    let inbound_service_configs = MessengerInboundServiceConfig::new(allowed_actions.into(), Some(10), Some(60 * 60 * 1_000));
    let configs = MessengerServicesTesterConfigs::new(
        SuffixConfig {
            capacity: 50,
            prune_start_threshold: Some(55),
            min_size_after_prune: None,
        },
        inbound_service_configs,
    );
    let mut service_tester = MessengerServiceTester::new_with_mock_action_service(configs);
    // END - Prep before test

    // START - Prepare basic candidates with various different types of on-commit actions
    // Candidate with on commit publish to kafka messages.
    let mut on_commit = MockOnCommitMessage::new();

    for _ in 0..5 {
        on_commit.insert_kafka_message("some-topic".to_owned(), None, None, get_default_kafka_payload());
    }
    let on_commit_value = on_commit.as_value();
    let candidate_with_on_commit: CandidateMessage = CandidateTestPayload::new().add_on_commit(&on_commit_value).build();
    assert!(candidate_with_on_commit.on_commit.is_some());
    // END - Prepare basic candidates with various different types of on-commit actions.

    let headers = HashMap::new();

    // ########### Send Candidate version = 9 - no commit actions ########### //
    let vers_14 = 14;
    let cm_14 = MockChannelMessage::new(&candidate_with_on_commit, vers_14);
    let cm_channel_msg_14 = cm_14.build_candidate_channel_message(&headers);

    service_tester.process_message_journey(Some(cm_channel_msg_14), None, None).await;

    // Because version 14 has `on_commit` actions but hasn't got decision, state will move to  `AwaitingDecision`.
    // ~~~~~ State Transition:  ⮕ `AwaitingDecision` ~~~~~ //
    let suffix = service_tester.get_suffix();
    suffix.assert_item_state(vers_14, &SuffixItemState::AwaitingDecision);

    suffix.assert_suffix_head_and_prune_index(vers_14, None);
    assert_eq!(suffix.suffix_length(), 1);

    let abort_outcome = build_mock_outcome(None, None);
    let dm_channel_msg_14 = cm_14.build_decision_channel_message(18, &abort_outcome, 0, &headers);

    service_tester.process_message_journey(None, Some(dm_channel_msg_14), None).await;

    let suffix = service_tester.get_suffix();
    suffix.assert_suffix_head_and_prune_index(vers_14, Some(0));
    suffix.assert_item_state(vers_14, &SuffixItemState::Complete(crate::suffix::SuffixItemCompleteStateReason::Aborted));
}

#[tokio::test]
async fn test_suffix_with_success_feedbacks_only() {
    let mut allowed_actions = AHashMap::new();
    allowed_actions.insert("publish".to_owned(), vec!["kafka".to_owned()]);

    let inbound_service_configs = MessengerInboundServiceConfig::new(allowed_actions.into(), Some(10), Some(60 * 60 * 1_000));
    let configs = MessengerServicesTesterConfigs::new(
        SuffixConfig {
            capacity: 50,
            prune_start_threshold: Some(55),
            min_size_after_prune: None,
        },
        inbound_service_configs,
    );
    let mut service_tester = MessengerServiceTester::new_with_mock_action_service(configs);

    let mut on_commit = MockOnCommitMessage::new();
    on_commit.insert_kafka_message("some-topic".to_owned(), None, None, get_default_kafka_payload());
    let on_commit_value = on_commit.as_value();

    let candidate: CandidateMessage = CandidateTestPayload::new().add_on_commit(&on_commit_value).build();
    assert!(candidate.on_commit.is_some());

    let headers = HashMap::new();
    // ########### Send candidate with version = 0 ########### //
    let cm_0 = MockChannelMessage::new(&candidate, 0);
    let cm_channel_msg_0 = cm_0.build_candidate_channel_message(&headers);

    service_tester.process_message_journey(Some(cm_channel_msg_0), None, None).await;

    // version 0 will not be inserted, hence suffix length should be 0
    let suffix = service_tester.get_suffix();
    assert_eq!(suffix.messages.len(), 0);
    // version 0 will not be inserted, hence head will be 0.
    // Prune index will be `None` as version 0 is ignored + decision not received.
    suffix.assert_suffix_head_and_prune_index(0, None);

    // ########### Send Candidate with version = 2 ########### //
    let cm_2 = MockChannelMessage::new(&candidate, 2);
    let cm_channel_msg_2 = cm_2.build_candidate_channel_message(&headers);
    service_tester.process_message_journey(Some(cm_channel_msg_2), None, None).await;

    // version 2 will be inserted, hence suffix length will be 1
    let suffix = service_tester.get_suffix();
    assert_eq!(suffix.messages.len(), 1);
    // version 2 will be inserted, hence head will be 2.
    // Prune index will be `None` as decision not received for version 2.
    suffix.assert_suffix_head_and_prune_index(2, None);
    // Version 2 state - `Awaiting Decision`.
    suffix.assert_item_state(2, &SuffixItemState::AwaitingDecision);

    // ########### Send Candidate version = 3 ########### //
    let cm_3 = MockChannelMessage::new(&candidate, 3);
    let cm_channel_msg_3 = cm_3.build_candidate_channel_message(&headers);
    service_tester.process_message_journey(Some(cm_channel_msg_3), None, None).await;

    // version 3 will be inserted, hence suffix length will be 2
    let suffix = service_tester.get_suffix();
    assert_eq!(suffix.messages.len(), 2);

    // version 3 will be inserted, but head will be still at 2.
    // Prune index will be `None` as decision not received for version 3.
    let suffix = service_tester.get_suffix();
    suffix.assert_suffix_head_and_prune_index(2, None);
    // Version 3 state - `Awaiting Decision`.
    suffix.assert_item_state(3, &SuffixItemState::AwaitingDecision);

    // ########### Send commit decision for Candidate version = 3 ########### //
    let commit_outcome = build_mock_outcome(None, Some(0));
    let dm_channel_msg_3 = cm_3.build_decision_channel_message(4, &commit_outcome, 0, &headers);

    // process the decision for version 3.
    service_tester.process_message_journey(None, Some(dm_channel_msg_3), None).await;

    // Version 3 state - `Processing`, because the safepoint is 0, so we don't have to wait for version 2 to be in `Processing` || `Completed(..)` state.
    let suffix = service_tester.get_suffix();
    suffix.assert_item_state(3, &SuffixItemState::Processing);

    // ########### Feedback processing for Candidate version = 3 ########### //
    // Nothing should happen as version 2 hasn't had decision yet.
    service_tester.process_message_journey(None, None, Some(JourneyConfig::new(true, 1))).await;

    // Version 3 state - `Complete(Processed)`, as the successful feedback was received.
    let suffix = service_tester.get_suffix();
    suffix.assert_item_state(3, &SuffixItemState::Complete(crate::suffix::SuffixItemCompleteStateReason::Processed));
    // Prune index will be still `None` as decision + processing not complete for version 2.
    suffix.assert_suffix_head_and_prune_index(2, None);

    // ########### Send Candidate version = 7 ########### //
    let cm_7 = MockChannelMessage::new(&candidate, 7);
    let cm_channel_msg_7 = cm_7.build_candidate_channel_message(&headers);

    // Send candidate and process in inbound_service.
    service_tester.process_message_journey(Some(cm_channel_msg_7), None, None).await;

    // version 7 will be inserted, hence suffix length will be 6
    let suffix = service_tester.get_suffix();
    assert_eq!(suffix.messages.len(), 6);
    // version 7 will be inserted, but head will be still at 2.
    // Prune index will be `None` as decision not received for version 7.
    suffix.assert_suffix_head_and_prune_index(2, None);
    // Version 7 state - `Awaiting Decision`.
    suffix.assert_item_state(7, &SuffixItemState::AwaitingDecision);

    // ########### Send commit decision for Candidate version = 7 with safepoint = version 2 ########### //
    let commit_outcome_safepoint_2 = build_mock_outcome(None, Some(2));
    let dm_channel_msg_7 = cm_7.build_decision_channel_message(8, &commit_outcome_safepoint_2, 0, &headers);

    // process the decision for version 7.
    service_tester.process_message_journey(None, Some(dm_channel_msg_7), None).await;

    // Version 7 state - `ReadyToProcess`, because the safepoint is 2, and version 2 is not in `Processing` || `Completed(..)` state.
    let suffix = service_tester.get_suffix();
    suffix.assert_item_state(7, &SuffixItemState::ReadyToProcess);

    // ########### Feedback processing for Candidate version = 7 ########### //
    // Nothing should happen as version 2 hasn't had decision yet.
    service_tester.process_message_journey(None, None, Some(JourneyConfig::new(true, 1))).await;

    // Version 7 state - `ReadyToProcess`, because of the safepoint condition with version 2, till version 2 is in `Processing` || `Completed(..)` state,
    // the state of version 7 will not change .
    let suffix = service_tester.get_suffix();
    suffix.assert_item_state(7, &SuffixItemState::ReadyToProcess);

    // ########### Send abort decision for Candidate version = 2 ########### //
    let abort_outcome = build_mock_outcome(None, None);
    let dm_channel_msg_2 = cm_2.build_decision_channel_message(9, &abort_outcome, 0, &headers);

    // Because the decision for version 2 is abort, the state will be moved to `Complete(Aborted)`.

    service_tester.process_message_journey(None, Some(dm_channel_msg_2), None).await;

    let suffix = service_tester.get_suffix();
    suffix.assert_item_state(2, &SuffixItemState::Complete(crate::suffix::SuffixItemCompleteStateReason::Aborted));
    // Since version 3 (index 1) will be new prune index, as everything till there is in `Compelete(..)` state.
    suffix.assert_suffix_head_and_prune_index(2, Some(1));

    // As version 2 has moved to `Complete(..)` state, version 7 will move to `Processing` state.
    suffix.assert_item_state(7, &SuffixItemState::Processing);

    // ########### Feedback processing for Candidate version = 7 (again) ########### //
    service_tester.process_message_journey(None, None, Some(JourneyConfig::new(true, 1))).await;

    // version 7 will move to `Complete(Processed)` state
    let suffix = service_tester.get_suffix();
    suffix.assert_item_state(7, &SuffixItemState::Complete(crate::suffix::SuffixItemCompleteStateReason::Processed));

    // Version 7 (index 5) will be new prune index, as everything till there is in `Compelete(..)` state.
    suffix.assert_suffix_head_and_prune_index(2, Some(5));
}

// The below test is a very exhaustive test which has candidate and decisions with respective versions and tests state transitions based on on_commit actions, feedback, decision outcome, and safepoint from decision.
// --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
//                      |                     |             Messenger Relavant            |       Certifier Decision     |                                                                                             |
//  Candidate version   | Decision version    | ----------------------------------------  | ---------------------------- | Final state                                                                                 |
//                      |                     | On commit actions?     | Feedback loop    | Decision        | Safepoint  |                                                                                             |
// --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
//    version 3         |   version 12        | has relavant on_commit | Error feedback   | commit decision | 0          | ✅ SuffixItemState::Complete(SuffixItemCompleteStateReason::ErrorProcessing)                |
//    version 9         |   version 11        | has no on_commit       | N/A              | commit decision | 0          | ✅ SuffixItemState::Complete(SuffixItemCompleteStateReason::NoCommitActions)                |
//    version 10        |   version 13        | no relavant on_commit  | N/A              | commit decision | 0          | ✅ SuffixItemState::Complete(SuffixItemCompleteStateReason::NoRelavantCommitActions)        |
//    version 14        |   version 16        | has relavant on_commit | N/A              | abort decision  | N/A        | ✅ SuffixItemState::Complete(SuffixItemCompleteStateReason::Aborted)                        |
//    version 15        |   version 19        | has relavant on_commit | Success feedback | commit decision | 0          | ✅ SuffixItemState::Complete(SuffixItemCompleteStateReason::Processed)                      |
//    version 20        |   version 21        | has relavant on_commit | Partial feedback | commit decision | 0          | 🛑 Wont move to final state and will be on SuffixItemState::PartiallyComplete               |
//    version 22        |   version 23        | has relavant on_commit | No feedback      | commit decision | 0          | 🛑 Wont move to final state and will be on SuffixItemState::Processing                      |
//    version 24        |   version 25        | has relavant on_commit | Success feedback | commit decision | 0          | ✅ SuffixItemState::Complete(SuffixItemCompleteStateReason::Processed)                      |
//    version 26        |   version 27        | has relavant on_commit | N/A              | no decision yet | N/A        | 🛑 Wont move to final state and will be on SuffixItemState::AwaitingDecision                |
//    version 28        |   version 29        | has relavant on_commit | Success feedback | commit decision | 3          | ✅ SuffixItemState::Complete(SuffixItemCompleteStateReason::Processed)                      |
//    version 32        |   version 33        | has relavant on_commit | Success feedback | commit decision | 22         | ✅ SuffixItemState::Complete(SuffixItemCompleteStateReason::Processed)                      |
//    version 34        |   version 35        | has relavant on_commit | N/A              | commit decision | 26         | 🛑 Wont move to final state and will be on SuffixItemState::ReadyToProcess                  |
// --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
#[tokio::test]
async fn test_suffix_exhaustive_state_transitions_without_pruning() {
    let mut allowed_actions = AHashMap::new();
    allowed_actions.insert("publish".to_owned(), vec!["kafka".to_owned()]);

    let inbound_service_configs = MessengerInboundServiceConfig::new(allowed_actions.into(), Some(10), Some(60 * 60 * 1_000));
    let configs = MessengerServicesTesterConfigs::new(
        SuffixConfig {
            capacity: 50,
            prune_start_threshold: Some(55),
            min_size_after_prune: None,
        },
        inbound_service_configs,
    );
    let mut service_tester = MessengerServiceTester::new_with_mock_action_service(configs);

    let headers = HashMap::new();

    // START - Prepare basic candidates with various different types of on-commit actions

    // Candidate with on commit publish to kafka messages.
    let mut on_commit = MockOnCommitMessage::new();

    let on_commit_count_5 = 5;
    for _ in 0..on_commit_count_5 {
        on_commit.insert_kafka_message("some-topic".to_owned(), None, None, get_default_kafka_payload());
    }
    let on_commit_value = on_commit.as_value();
    let candidate_with_on_commit: CandidateMessage = CandidateTestPayload::new().add_on_commit(&on_commit_value).build();
    assert!(candidate_with_on_commit.on_commit.is_some());

    // Candidate with no on-commit action
    let candidate_with_no_on_commit: CandidateMessage = CandidateTestPayload::new().build();
    assert!(candidate_with_no_on_commit.on_commit.is_none());
    // Candidate with no supported on-commit action
    let on_commit = MockOnCommitMessage::build_from_str(r#"{"notSuportedAction": {"name": "Kindred"}}"#);
    let on_commit_value = on_commit.as_value();

    let candidate_with_irrelevant_on_commit: CandidateMessage = CandidateTestPayload::new().add_on_commit(&on_commit_value).build();
    assert!(candidate_with_irrelevant_on_commit.on_commit.is_some());

    // End - Prepare basic candidates with various different types of on-commit actions

    let commit_outcome = build_mock_outcome(None, Some(0));
    // --------------------------------------------------------------------------------------------------------------
    //
    //   Handling of state transition scenarios from the above table for version 3, 9 and 10.
    //
    // --------------------------------------------------------------------------------------------------------------

    // ################ version 3 Candidate #####################/
    let vers_3 = 3;
    let cm_3 = MockChannelMessage::new(&candidate_with_on_commit, vers_3);
    let cm_channel_msg_3 = cm_3.build_candidate_channel_message(&headers);

    // ################ version 9 Candidate #####################/
    let vers_9 = 9;
    let cm_9 = MockChannelMessage::new(&candidate_with_no_on_commit, vers_9);
    let cm_channel_msg_9 = cm_9.build_candidate_channel_message(&headers);

    // ################ version 10 Candidate #####################/
    let vers_10 = 10;
    let cm_10 = MockChannelMessage::new(&candidate_with_irrelevant_on_commit, vers_10);
    let cm_channel_msg_10 = cm_10.build_candidate_channel_message(&headers);

    service_tester.process_message_journey(Some(cm_channel_msg_3), None, None).await;
    service_tester.process_message_journey(Some(cm_channel_msg_9), None, None).await;
    service_tester.process_message_journey(Some(cm_channel_msg_10), None, None).await;

    let suffix = service_tester.get_suffix();
    suffix.assert_item_state(vers_3, &SuffixItemState::AwaitingDecision);
    suffix.assert_item_state(
        vers_9,
        &SuffixItemState::Complete(crate::suffix::SuffixItemCompleteStateReason::NoCommitActions),
    );
    suffix.assert_item_state(
        vers_10,
        &SuffixItemState::Complete(crate::suffix::SuffixItemCompleteStateReason::NoRelavantCommitActions),
    );

    // ################ version 9, 3, 10 Decisions the respective order #####################/

    // Since no item is marked decided or actions processed, the prune_index will be None.
    suffix.assert_suffix_head_and_prune_index(3, None);

    let dm_channel_msg_9 = cm_9.build_decision_channel_message(11, &commit_outcome, 0, &headers);

    let mut headers_with_error_feedback = headers.clone();
    headers_with_error_feedback.insert("feedbackType".to_owned(), FeedbackTypeHeader::Error.to_string());
    let dm_channel_msg_3 = cm_3.build_decision_channel_message(12, &commit_outcome, 0, &headers_with_error_feedback);

    let dm_channel_msg_10 = cm_10.build_decision_channel_message(13, &commit_outcome, 0, &headers);

    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // ######                       Processing candidates 3, 9 and 10 decision messages                                 #######
    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    // Process Decision for version 9.
    service_tester.process_message_journey(None, Some(dm_channel_msg_9), None).await;

    // Process Decision for version 3 - We know it has valid `on_commits` to process and therefore we run -
    // - Abcast -> inbound_service (mark the suffix as decided + pass the actions to next service)
    // - action_service - to process the actions received
    // - inbound_service - to receive the feedback from action service.
    service_tester
        .process_message_journey(None, Some(dm_channel_msg_3), Some(JourneyConfig::new(true, on_commit_count_5)))
        .await;

    // Process Decision for version 10.
    service_tester.process_message_journey(None, Some(dm_channel_msg_10), None).await;

    // Assert suffix and version item states after everything related to version 3 and 9 are processed.
    let suffix = service_tester.get_suffix();
    suffix.assert_item_state(
        vers_9,
        &SuffixItemState::Complete(crate::suffix::SuffixItemCompleteStateReason::NoCommitActions),
    );
    suffix.assert_item_state(
        vers_3,
        &SuffixItemState::Complete(crate::suffix::SuffixItemCompleteStateReason::ErrorProcessing),
    );
    suffix.assert_item_state(
        vers_10,
        &SuffixItemState::Complete(crate::suffix::SuffixItemCompleteStateReason::NoRelavantCommitActions),
    );

    // suffix head will be at 3 and prune index will be at 7 (version 10)
    suffix.assert_suffix_head_and_prune_index(3, Some(7));

    // --------------------------------------------------------------------------------------------------------------
    //
    //   Handling of state transition scenarios from the above table for version 14, 15, 20 and 22.
    //
    // --------------------------------------------------------------------------------------------------------------

    // ################ version 14 Candidate and Decision #####################/
    let vers_14 = 14;

    let cm_14 = MockChannelMessage::new(&candidate_with_on_commit, vers_14);
    let cm_channel_msg_14 = cm_14.build_candidate_channel_message(&headers);

    let abort_outcome = build_mock_outcome(None, None);
    let dm_channel_msg_14 = cm_14.build_decision_channel_message(16, &abort_outcome, 0, &headers);

    // ################ version 15 Candidate and Decision #####################/
    let vers_15 = 15;

    let cm_15 = MockChannelMessage::new(&candidate_with_on_commit, vers_15);
    let cm_channel_msg_15 = cm_15.build_candidate_channel_message(&headers);

    let dm_channel_msg_15 = cm_15.build_decision_channel_message(19, &commit_outcome, 0, &headers);

    // ################ version 20 Candidate and Decision #####################/
    let vers_20 = 20;

    let cm_20 = MockChannelMessage::new(&candidate_with_on_commit, vers_20);
    let cm_channel_msg_20 = cm_20.build_candidate_channel_message(&headers);

    // let commit_outcome = build_mock_outcome(None, Some(0));
    let mut headers_with_partial_feedback = headers.clone();
    headers_with_partial_feedback.insert("feedbackType".to_owned(), FeedbackTypeHeader::Partial.to_string());
    let dm_channel_msg_20 = cm_20.build_decision_channel_message(21, &commit_outcome, 0, &headers_with_partial_feedback);

    // ################ version 22 Candidate and Decision #####################/
    let vers_22 = 22;

    let cm_22 = MockChannelMessage::new(&candidate_with_on_commit, vers_22);
    let cm_channel_msg_22 = cm_22.build_candidate_channel_message(&headers);

    // let commit_outcome = build_mock_outcome(None, Some(0));
    let mut headers_with_no_feedback = headers.clone();
    headers_with_no_feedback.insert("feedbackType".to_owned(), FeedbackTypeHeader::NoFeedback.to_string());
    let dm_channel_msg_22 = cm_22.build_decision_channel_message(23, &commit_outcome, 0, &headers_with_no_feedback);

    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // ######                       Processing candidates 14, 15, 20, 22 related messages                               #######
    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    service_tester
        .process_message_journey(Some(cm_channel_msg_14), Some(dm_channel_msg_14), Some(JourneyConfig::new(true, 0)))
        .await;
    service_tester
        .process_message_journey(
            Some(cm_channel_msg_15),
            Some(dm_channel_msg_15),
            Some(JourneyConfig::new(true, on_commit_count_5)),
        )
        .await;
    service_tester
        .process_message_journey(Some(cm_channel_msg_20), Some(dm_channel_msg_20), Some(JourneyConfig::new(true, 1)))
        .await;
    service_tester
        .process_message_journey(Some(cm_channel_msg_22), Some(dm_channel_msg_22), Some(JourneyConfig::new(true, 1)))
        .await;

    let suffix = service_tester.get_suffix();
    suffix.assert_item_state(vers_14, &SuffixItemState::Complete(crate::suffix::SuffixItemCompleteStateReason::Aborted));
    suffix.assert_item_state(vers_15, &SuffixItemState::Complete(crate::suffix::SuffixItemCompleteStateReason::Processed));
    suffix.assert_item_state(vers_20, &SuffixItemState::PartiallyComplete);
    suffix.assert_item_state(vers_22, &SuffixItemState::Processing);

    // suffix head will be at 3 and prune index will be at 12 (version 15). Can't move to index of version 20 and 22, as they are not Complete.
    suffix.assert_suffix_head_and_prune_index(3, Some(12));

    // --------------------------------------------------------------------------------------------------------------
    //
    //   Handling of state transition scenarios from the above table for version 24, 26 and 28.
    //
    // --------------------------------------------------------------------------------------------------------------

    // ################ version 24 Candidate and Decision #####################/
    let vers_24 = 24;

    let cm_24 = MockChannelMessage::new(&candidate_with_on_commit, vers_24);
    let cm_channel_msg_24 = cm_24.build_candidate_channel_message(&headers);

    // let commit_outcome = build_mock_outcome(None, Some(0));
    let dm_channel_msg_24 = cm_24.build_decision_channel_message(25, &commit_outcome, 0, &headers);

    // ################ version 26 Candidate and Decision #####################/
    let vers_26 = 26;

    let cm_26 = MockChannelMessage::new(&candidate_with_on_commit, vers_26);
    let cm_channel_msg_26 = cm_26.build_candidate_channel_message(&headers);

    // ################ version 28 Candidate and Decision #####################/
    let vers_28 = 28;

    let cm_28 = MockChannelMessage::new(&candidate_with_on_commit, vers_28);
    let cm_channel_msg_28 = cm_28.build_candidate_channel_message(&headers);

    // Produces a commit outcome with safepoint as version 3.
    let commit_outcome_safepoint_3 = build_mock_outcome(None, Some(3));
    let dm_channel_msg_28 = cm_28.build_decision_channel_message(27, &commit_outcome_safepoint_3, 0, &headers);

    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // ######                         Processing candidates 24, 26, 28 related messages                                 #######
    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    service_tester
        .process_message_journey(
            Some(cm_channel_msg_24),
            Some(dm_channel_msg_24),
            Some(JourneyConfig::new(true, on_commit_count_5)),
        )
        .await;
    service_tester.process_message_journey(Some(cm_channel_msg_26), None, None).await;
    service_tester
        .process_message_journey(
            Some(cm_channel_msg_28),
            Some(dm_channel_msg_28),
            Some(JourneyConfig::new(true, on_commit_count_5)),
        )
        .await;

    let suffix = service_tester.get_suffix();
    suffix.assert_item_state(vers_24, &SuffixItemState::Complete(crate::suffix::SuffixItemCompleteStateReason::Processed));
    suffix.assert_item_state(vers_26, &SuffixItemState::AwaitingDecision);
    suffix.assert_item_state(vers_28, &SuffixItemState::Complete(crate::suffix::SuffixItemCompleteStateReason::Processed));

    // suffix head will be at 3 and prune index will be at 12 (version 15). Can't move further as version 20 and 22 will not complete.
    suffix.assert_suffix_head_and_prune_index(3, Some(12));

    // --------------------------------------------------------------------------------------------------------------
    //
    //   Handling of state transition scenarios from the above table for version 32 and 34.
    //
    // --------------------------------------------------------------------------------------------------------------

    // ################ version 32 Candidate and Decision #####################/
    let vers_32 = 32;

    let cm_32 = MockChannelMessage::new(&candidate_with_on_commit, vers_32);
    let cm_channel_msg_32 = cm_32.build_candidate_channel_message(&headers);

    // safepoint 22 is already in `Processing` state, so this can proceed.
    let commit_outcome_safepoint_22 = build_mock_outcome(None, Some(22));
    let dm_channel_msg_32 = cm_32.build_decision_channel_message(33, &commit_outcome_safepoint_22, 0, &headers);

    // ################ version 34 Candidate and Decision #####################/
    let vers_34 = 34;

    let cm_34 = MockChannelMessage::new(&candidate_with_on_commit, vers_34);
    let cm_channel_msg_34 = cm_34.build_candidate_channel_message(&headers);

    // safepoint 26 is  in `AwaitingDecision` state, so this cannot proceed.
    let commit_outcome_safepoint_26 = build_mock_outcome(None, Some(26));
    let dm_channel_msg_34 = cm_34.build_decision_channel_message(35, &commit_outcome_safepoint_26, 0, &headers);

    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // ######                         Processing candidates 32 and 34 related messages                                  #######
    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    service_tester
        .process_message_journey(
            Some(cm_channel_msg_32),
            Some(dm_channel_msg_32),
            Some(JourneyConfig::new(true, on_commit_count_5)),
        )
        .await;
    service_tester
        .process_message_journey(Some(cm_channel_msg_34), Some(dm_channel_msg_34), Some(JourneyConfig::new(true, 0)))
        .await;

    let suffix = service_tester.get_suffix();
    suffix.assert_item_state(vers_32, &SuffixItemState::Complete(crate::suffix::SuffixItemCompleteStateReason::Processed));
    suffix.assert_item_state(vers_34, &SuffixItemState::ReadyToProcess);

    // suffix head will be at 3 and prune index will be at 12 (version 15). Can't move to index of version 20 and 22, as they are not Complete.
    suffix.assert_suffix_head_and_prune_index(3, Some(12));
}

// ############################################################# END - state transition related tests  ############################################################# //

// TODO: GK - Run `test_suffix_exhaustive_state_transitions_without_pruning` with smaller prune_threshold to see how pruning behaves through all the above scenarios.

// TODO: GK - Test commit offset logic

// TODO: GK - Test the interval arm - how it updates the prune_index and commit_offset
