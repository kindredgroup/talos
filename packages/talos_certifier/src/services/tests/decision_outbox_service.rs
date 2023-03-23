use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::{
    errors::{SystemServiceError, SystemServiceErrorKind},
    model::{Decision, DecisionMessage},
    ports::{
        common::SharedPortTraits,
        errors::{DecisionStoreError, DecisionStoreErrorKind},
        DecisionStore, MessagePublisher,
    },
    SystemMessage,
};
use async_trait::async_trait;
use tokio::{
    sync::{broadcast, mpsc},
    time::{sleep_until, Instant},
};

use crate::{
    core::{System, SystemService},
    services::DecisionOutboxService,
};

#[derive(Debug, Clone)]
struct MockDecisionStore {
    decision_message: Arc<Mutex<Option<DecisionMessage>>>,
}

#[async_trait]
impl DecisionStore for MockDecisionStore {
    type Decision = DecisionMessage;

    async fn get_decision(&self, _key: String) -> Result<Option<Self::Decision>, DecisionStoreError> {
        let decision = self.decision_message.lock().unwrap().take().unwrap();
        Ok(Some(decision))
    }

    async fn insert_decision(&self, _key: String, decision: Self::Decision) -> Result<Self::Decision, DecisionStoreError> {
        let mut k = self.decision_message.lock().unwrap();
        *k = Some(decision.clone());

        // = Some(decision);
        Ok(decision)
    }
}

#[async_trait]
impl SharedPortTraits for MockDecisionStore {
    async fn is_healthy(&self) -> bool {
        true
    }
    async fn shutdown(&self) -> bool {
        false
    }
}
#[derive(Debug, Clone)]
struct MockDecisionPublisher;

#[async_trait]
impl MessagePublisher for MockDecisionPublisher {
    async fn publish_message(&self, _key: &str, _value: &str, _headers: Option<HashMap<String, String>>) -> Result<(), SystemServiceError> {
        Ok(())
    }
}

#[async_trait]
impl SharedPortTraits for MockDecisionPublisher {
    async fn is_healthy(&self) -> bool {
        true
    }
    async fn shutdown(&self) -> bool {
        false
    }
}

// Test 1:- Receive Decision Message and save and publish it.

#[tokio::test]
async fn test_candidate_message_create_decision_message() {
    let (do_channel_tx, do_channel_rx) = mpsc::channel(2);

    let decision_message = Arc::new(Mutex::new(None));

    let mock_decision_store = MockDecisionStore {
        decision_message: Arc::clone(&decision_message),
    };
    let mock_decision_publisher = MockDecisionPublisher {};

    let (system_notifier, _system_rx) = broadcast::channel(10);

    let system = System {
        system_notifier,
        is_shutdown: false,
    };

    //clones
    let do_channel_tx_clone = do_channel_tx.clone();

    let mut dob_svc = DecisionOutboxService::new(
        do_channel_tx,
        do_channel_rx,
        Arc::new(Box::new(mock_decision_store)),
        Arc::new(Box::new(mock_decision_publisher)),
        system,
    );

    // sending a decision into decision outbox service
    tokio::spawn(async move {
        do_channel_tx_clone
            .send(crate::core::DecisionOutboxChannelMessage::Decision(DecisionMessage {
                xid: "test-xid-1".to_owned(),
                agent: "test-agent-1".to_owned(),
                cohort: "test-cohort-1".to_owned(),
                decision: Decision::Committed,
                suffix_start: 2,
                version: 4,
                duplicate_version: None,
                safepoint: Some(3),
                conflicts: None,
            }))
            .await
            .unwrap();
    });

    let result = dob_svc.run().await;

    sleep_until(Instant::now() + Duration::from_millis(50)).await;
    let dm = decision_message.lock().unwrap().take().unwrap();
    assert_eq!(dm.xid, "test-xid-1".to_owned());

    assert!(result.is_ok());
}

// Test 2:- Receive Multiple Decision Message and save and publish them asynchronously in their own thread.
#[tokio::test]
async fn test_save_and_publish_multiple_decisions() {
    let (do_channel_tx, do_channel_rx) = mpsc::channel(2);

    let decision_message = Arc::new(Mutex::new(None));

    let mock_decision_store = MockDecisionStore {
        decision_message: Arc::clone(&decision_message),
    };
    let mock_decision_publisher = MockDecisionPublisher {};

    let (system_notifier, _system_rx) = broadcast::channel(10);

    let system = System {
        system_notifier,
        is_shutdown: false,
    };

    //clones
    let do_channel_tx_clone = do_channel_tx.clone();
    let do_channel_tx_clone_2 = do_channel_tx.clone();

    let mut dob_svc = DecisionOutboxService::new(
        do_channel_tx,
        do_channel_rx,
        Arc::new(Box::new(mock_decision_store)),
        Arc::new(Box::new(mock_decision_publisher)),
        system,
    );

    // sending a decision into decision outbox service
    tokio::spawn(async move {
        do_channel_tx_clone
            .send(crate::core::DecisionOutboxChannelMessage::Decision(DecisionMessage {
                xid: "test-xid-1".to_owned(),
                agent: "test-agent-1".to_owned(),
                cohort: "test-cohort-1".to_owned(),
                decision: Decision::Committed,
                suffix_start: 2,
                version: 4,
                duplicate_version: None,
                safepoint: Some(3),
                conflicts: None,
            }))
            .await
            .unwrap();
    });

    tokio::spawn(async move {
        do_channel_tx_clone_2
            .send(crate::core::DecisionOutboxChannelMessage::Decision(DecisionMessage {
                xid: "test-xid-2".to_owned(),
                agent: "test-agent-1".to_owned(),
                cohort: "test-cohort-1".to_owned(),
                decision: Decision::Committed,
                suffix_start: 2,
                version: 4,
                duplicate_version: None,
                safepoint: Some(3),
                conflicts: None,
            }))
            .await
            .unwrap();
    });

    let _result = dob_svc.run().await;

    sleep_until(Instant::now() + Duration::from_millis(50)).await;
    let dm = decision_message.lock().unwrap().take().unwrap();
    assert_eq!(dm.xid, "test-xid-1".to_owned());

    let _result = dob_svc.run().await;

    sleep_until(Instant::now() + Duration::from_millis(50)).await;
    let dm = decision_message.lock().unwrap().take().unwrap();
    assert_eq!(dm.xid, "test-xid-2".to_owned());
}

// Test 3:- Capture error to insert decision to datastore in child thread

#[derive(Debug, Clone)]
struct MockDecisionStoreWithError {
    decision_message: Arc<Mutex<Option<DecisionMessage>>>,
}

#[async_trait]
impl DecisionStore for MockDecisionStoreWithError {
    type Decision = DecisionMessage;

    async fn get_decision(&self, _key: String) -> Result<Option<Self::Decision>, DecisionStoreError> {
        let decision = self.decision_message.lock().unwrap().take().unwrap();
        Ok(Some(decision))
    }

    async fn insert_decision(&self, _key: String, _decision: Self::Decision) -> Result<Self::Decision, DecisionStoreError> {
        Err(DecisionStoreError {
            kind: DecisionStoreErrorKind::ParseError,
            data: None,
            reason: "Unhandle parse error".to_string(),
        })
    }
}

#[async_trait]
impl SharedPortTraits for MockDecisionStoreWithError {
    async fn is_healthy(&self) -> bool {
        true
    }
    async fn shutdown(&self) -> bool {
        false
    }
}

#[tokio::test]
async fn test_capture_child_thread_dberror() {
    let (do_channel_tx, do_channel_rx) = mpsc::channel(2);

    let decision_message = Arc::new(Mutex::new(None));

    let mock_decision_store = MockDecisionStoreWithError {
        decision_message: Arc::clone(&decision_message),
    };
    let mock_decision_publisher = MockDecisionPublisher {};

    let (system_notifier, mut system_rx) = broadcast::channel(10);

    let system = System {
        system_notifier,
        is_shutdown: false,
    };

    //clones
    let do_channel_tx_clone = do_channel_tx.clone();

    let mut dob_svc = DecisionOutboxService::new(
        do_channel_tx,
        do_channel_rx,
        Arc::new(Box::new(mock_decision_store)),
        Arc::new(Box::new(mock_decision_publisher)),
        system,
    );

    // sending a decision into decision outbox service
    tokio::spawn(async move {
        do_channel_tx_clone
            .send(crate::core::DecisionOutboxChannelMessage::Decision(DecisionMessage {
                xid: "test-xid-1".to_owned(),
                agent: "test-agent-1".to_owned(),
                cohort: "test-cohort-1".to_owned(),
                decision: Decision::Committed,
                suffix_start: 2,
                version: 4,
                duplicate_version: None,
                safepoint: Some(3),
                conflicts: None,
            }))
            .await
            .unwrap();
    });

    let _result = dob_svc.run().await;

    if let Ok(SystemMessage::ShutdownWithError(error)) = system_rx.recv().await {
        assert!(error.kind == SystemServiceErrorKind::DBError);
    }
}

// Test 4:- Capture error to publish decision originating from child thread.
#[derive(Debug, Clone)]
struct MockDecisionPublisherWithError;

#[async_trait]
impl MessagePublisher for MockDecisionPublisherWithError {
    async fn publish_message(&self, _key: &str, _value: &str, _headers: Option<HashMap<String, String>>) -> Result<(), SystemServiceError> {
        Err(SystemServiceError {
            kind: SystemServiceErrorKind::MessagePublishError,
            reason: "Failed to Publish".to_string(),
            data: None,
            service: "Some svc".to_string(),
        })
    }
}

#[async_trait]
impl SharedPortTraits for MockDecisionPublisherWithError {
    async fn is_healthy(&self) -> bool {
        true
    }
    async fn shutdown(&self) -> bool {
        false
    }
}
#[tokio::test]
async fn test_capture_publish_error() {
    let mock_decision_publisher = MockDecisionPublisherWithError {};

    let decision_message = DecisionMessage {
        xid: "test-xid-1".to_owned(),
        agent: "test-agent-1".to_owned(),
        cohort: "test-cohort-1".to_owned(),
        decision: Decision::Committed,
        suffix_start: 2,
        version: 4,
        duplicate_version: None,
        safepoint: Some(3),
        conflicts: None,
    };

    if let Err(publish_error) = DecisionOutboxService::publish_decision(&Arc::new(Box::new(mock_decision_publisher)), &decision_message).await {
        assert!(publish_error.kind == SystemServiceErrorKind::MessagePublishError);
    }
}

#[derive(Debug, Clone)]
struct MockDecisionStoreHashMap {
    decision_message: Arc<Mutex<HashMap<String, DecisionMessage>>>,
}

#[async_trait]
impl DecisionStore for MockDecisionStoreHashMap {
    type Decision = DecisionMessage;

    async fn get_decision(&self, key: String) -> Result<Option<Self::Decision>, DecisionStoreError> {
        let decision_store = self.decision_message.lock().unwrap();
        let decision = decision_store.get(&key);
        Ok(decision.cloned())
    }

    async fn insert_decision(&self, key: String, decision: Self::Decision) -> Result<Self::Decision, DecisionStoreError> {
        let mut decision_store = self.decision_message.lock().unwrap();

        match decision_store.insert(key, decision.clone()) {
            Some(decision_old) => Ok(decision_old),
            _ => Ok(decision),
        }
    }
}

#[async_trait]
impl SharedPortTraits for MockDecisionStoreHashMap {
    async fn is_healthy(&self) -> bool {
        true
    }
    async fn shutdown(&self) -> bool {
        false
    }
}
#[tokio::test]
async fn test_duplicate_version_found_in_db() {
    let mock_db: Box<dyn DecisionStore<Decision = DecisionMessage> + Send + Sync> = Box::new(MockDecisionStoreHashMap {
        decision_message: Arc::new(Mutex::new(HashMap::new())),
    });

    let datastore = Arc::new(mock_db);

    let decision_message = DecisionMessage {
        xid: "test-xid-1".to_owned(),
        agent: "test-agent-1".to_owned(),
        cohort: "test-cohort-1".to_owned(),
        decision: Decision::Committed,
        suffix_start: 2,
        version: 4,
        duplicate_version: None,
        safepoint: Some(3),
        conflicts: None,
    };

    let result_first_decision = DecisionOutboxService::save_decision_to_xdb(&datastore, &decision_message).await;

    assert!(result_first_decision.is_ok());

    let decision_message_duplicate_same_xid = DecisionMessage {
        xid: "test-xid-1".to_owned(),
        agent: "test-agent-2".to_owned(),
        cohort: "test-cohort-1".to_owned(),
        decision: Decision::Committed,
        suffix_start: 5,
        version: 8,
        duplicate_version: None,
        safepoint: Some(3),
        conflicts: None,
    };
    let result_duplicate_decision = DecisionOutboxService::save_decision_to_xdb(&datastore, &decision_message_duplicate_same_xid).await;

    assert!(result_duplicate_decision.is_ok());

    let first_msg = result_first_decision.unwrap();
    let duplicate_msg = result_duplicate_decision.unwrap();

    assert_eq!(first_msg.version, 4);
    assert_eq!(first_msg.version, duplicate_msg.version);
}
