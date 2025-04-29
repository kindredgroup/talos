use std::sync::{atomic::AtomicI64, Arc};

use ahash::{HashMap, HashMapExt};
use async_trait::async_trait;
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
};

use crate::{
    core::{CandidateChannelMessage, System, SystemService},
    errors::SystemServiceError,
    model::CandidateMessage,
    ports::{common::SharedPortTraits, errors::MessageReceiverError, MessageReciever},
    services::{MessageReceiverService, MessageReceiverServiceConfig},
    ChannelMessage,
};

struct MockReciever {
    consumer: mpsc::Receiver<ChannelMessage<CandidateMessage>>,
    offset: i64,
}

impl MockReciever {
    pub fn new(consumer: mpsc::Receiver<ChannelMessage<CandidateMessage>>) -> Self {
        Self { consumer, offset: 0 }
    }
}

#[async_trait]
impl MessageReciever for MockReciever {
    type Message = ChannelMessage<CandidateMessage>;

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

#[tokio::test]
async fn test_consume_message() {
    let (mock_channel_tx, mock_channel_rx) = mpsc::channel(2);
    let (msg_channel_tx, mut msg_channel_rx) = mpsc::channel(2);

    let mock_receiver = MockReciever::new(mock_channel_rx);

    let (system_notifier, _system_rx) = broadcast::channel(10);

    let system = System {
        system_notifier,
        is_shutdown: false,
        name: "test-system".to_string(),
    };

    let commit_offset: Arc<AtomicI64> = Arc::new(0.into());

    let mut msg_receiver = MessageReceiverService::new(
        Box::new(mock_receiver),
        msg_channel_tx,
        commit_offset,
        system,
        MessageReceiverServiceConfig {
            commit_frequency_ms: 10_000,
            min_commit_threshold: 50_000,
        },
    );

    let candidate_message = CandidateMessage {
        xid: "xid-1".to_string(),
        version: 8,
        agent: "agent-1".to_string(),
        cohort: "cohort-1".to_string(),
        snapshot: 5,
        readvers: vec![],
        readset: vec![],
        writeset: vec!["ksp:w1".to_owned()],
        metadata: None,
        certification_started_at: 0,
        request_created_at: 0,
        published_at: 0,
        received_at: 0,
    };

    mock_channel_tx
        .send(ChannelMessage::Candidate(
            CandidateChannelMessage {
                message: candidate_message,
                headers: HashMap::new(),
            }
            .into(),
        ))
        .await
        .unwrap();

    let result = msg_receiver.run().await;

    assert!(result.is_ok());

    if let Some(ChannelMessage::Candidate(candidate)) = msg_channel_rx.recv().await {
        assert_eq!(candidate.message.version, 8);
        assert_eq!(candidate.message.xid, "xid-1".to_string());
    }
}

#[tokio::test]
async fn test_consume_message_error() {
    let (mock_channel_tx, mock_channel_rx) = mpsc::channel(2);
    let (msg_channel_tx, mut msg_channel_rx) = mpsc::channel(2);

    let mock_receiver = MockReciever::new(mock_channel_rx);

    let (system_notifier, _system_rx) = broadcast::channel(10);

    let system = System {
        system_notifier,
        is_shutdown: false,
        name: "test-system".to_string(),
    };
    let commit_offset: Arc<AtomicI64> = Arc::new(0.into());

    let mut msg_receiver = MessageReceiverService::new(
        Box::new(mock_receiver),
        msg_channel_tx,
        commit_offset,
        system,
        MessageReceiverServiceConfig {
            commit_frequency_ms: 10_000,
            min_commit_threshold: 50_000,
        },
    );
    let candidate_message = CandidateMessage {
        xid: "xid-1".to_string(),
        version: 8,
        agent: "agent-1".to_string(),
        cohort: "cohort-1".to_string(),
        snapshot: 5,
        readvers: vec![],
        readset: vec![],
        writeset: vec!["ksp:w1".to_owned()],
        metadata: None,
        certification_started_at: 0,
        request_created_at: 0,
        published_at: 0,
        received_at: 0,
    };
    mock_channel_tx
        .send(ChannelMessage::Candidate(
            CandidateChannelMessage {
                message: candidate_message,
                headers: HashMap::new(),
            }
            .into(),
        ))
        .await
        .unwrap();

    msg_channel_rx.close();

    let result = msg_receiver.run().await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_commit_offset_updation() {
    let (_mock_channel_tx, mock_channel_rx) = mpsc::channel(2);
    let (msg_channel_tx, mut _msg_channel_rx) = mpsc::channel(2);

    let mock_receiver = MockReciever::new(mock_channel_rx);

    let (system_notifier, _system_rx) = broadcast::channel(10);

    let system = System {
        system_notifier,
        is_shutdown: false,
        name: "test-system".to_string(),
    };
    let commit_offset: Arc<AtomicI64> = Arc::new(0.into());

    let mut msg_receiver = MessageReceiverService::new(
        Box::new(mock_receiver),
        msg_channel_tx,
        commit_offset.clone(),
        system,
        MessageReceiverServiceConfig {
            commit_frequency_ms: 10_000,
            min_commit_threshold: 50_000,
        },
    );

    commit_offset.store(50, std::sync::atomic::Ordering::Relaxed);
    // since the offset stored is 50, which doesn't pass the min_commit_threshold of 50_000, the below fn returns `None`.
    assert!(msg_receiver.get_commit_offset().is_none());

    commit_offset.store(59_990, std::sync::atomic::Ordering::Relaxed);
    // since the offset stored is 59_999, which passes the min_commit_threshold of 50_000, the below fn returns Some(9_990).
    assert_eq!(msg_receiver.get_commit_offset(), Some(9_990));

    // When min_commit_threshold is set to 0.
    msg_receiver.config.min_commit_threshold = 0;

    commit_offset.store(0, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(msg_receiver.get_commit_offset(), Some(0));

    commit_offset.store(50, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(msg_receiver.get_commit_offset(), Some(50));

    commit_offset.store(59_990, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(msg_receiver.get_commit_offset(), Some(59_990));
}
