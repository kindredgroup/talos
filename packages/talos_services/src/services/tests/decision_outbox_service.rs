use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use talos_core::{
    errors::{SystemServiceError, SystemServiceErrorKind},
    model::decision_message::{Decision, DecisionMessage},
    ports::{common::SharedPortTraits, decision_store::DecisionStore, errors::DecisionStoreError, message::MessagePublisher},
};
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

    async fn insert_decision(&mut self, _key: String, decision: Self::Decision) -> Result<Option<Self::Decision>, DecisionStoreError> {
        let mut k = self.decision_message.lock().unwrap();
        *k = Some(decision);

        // = Some(decision);
        Ok(None)
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
        Box::new(mock_decision_store),
        Box::new(mock_decision_publisher),
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
        Box::new(mock_decision_store),
        Box::new(mock_decision_publisher),
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

    async fn insert_decision(&mut self, _key: String, _decision: Self::Decision) -> Result<Option<Self::Decision>, DecisionStoreError> {
        Err(DecisionStoreError {
            kind: talos_core::ports::errors::DecisionStoreErrorKind::ParseError,
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
        Box::new(mock_decision_store),
        Box::new(mock_decision_publisher),
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
                safepoint: Some(3),
                conflicts: None,
            }))
            .await
            .unwrap();
    });

    let _result = dob_svc.run().await;
    let result = dob_svc.run().await;

    assert!(result.is_err());

    if let Err(error) = result {
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
async fn test_capture_child_thread_publish_error() {
    let (do_channel_tx, do_channel_rx) = mpsc::channel(2);

    let decision_message = Arc::new(Mutex::new(None));

    let mock_decision_store = MockDecisionStore {
        decision_message: Arc::clone(&decision_message),
    };
    let mock_decision_publisher = MockDecisionPublisherWithError {};

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
        Box::new(mock_decision_store),
        Box::new(mock_decision_publisher),
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
                safepoint: Some(3),
                conflicts: None,
            }))
            .await
            .unwrap();
    });

    let _ = dob_svc.run().await;
    let result = dob_svc.run().await;

    assert!(result.is_err());

    if let Err(error) = result {
        assert!(error.kind == SystemServiceErrorKind::MessagePublishError);
    }
}
