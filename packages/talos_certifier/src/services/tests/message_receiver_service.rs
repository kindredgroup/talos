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
    services::MessageReceiverService,
    ChannelMessage,
};

struct MockReciever {
    consumer: mpsc::Receiver<ChannelMessage<CandidateMessage>>,
    offset: Option<u64>,
}

#[async_trait]
impl MessageReciever for MockReciever {
    type Message = ChannelMessage<CandidateMessage>;

    async fn consume_message(&mut self) -> Result<Option<Self::Message>, MessageReceiverError> {
        let msg = self.consumer.recv().await.unwrap();

        let vers = match &msg {
            ChannelMessage::Candidate(msg) => Some(msg.message.version),
            ChannelMessage::Decision(decision) => Some(decision.decision_version),
        };

        self.offset = vers;

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
    fn update_offset_to_commit(&mut self, _version: i64) -> Result<(), Box<SystemServiceError>> {
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

    let mock_receiver = MockReciever {
        consumer: mock_channel_rx,
        offset: None,
    };

    let (system_notifier, _system_rx) = broadcast::channel(10);

    let system = System {
        system_notifier,
        is_shutdown: false,
        name: "test-system".to_string(),
    };

    let commit_offset: Arc<AtomicI64> = Arc::new(0.into());

    let mut msg_receiver = MessageReceiverService::new(Box::new(mock_receiver), msg_channel_tx, commit_offset, system);

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

    let mock_receiver = MockReciever {
        consumer: mock_channel_rx,
        offset: None,
    };

    let (system_notifier, _system_rx) = broadcast::channel(10);

    let system = System {
        system_notifier,
        is_shutdown: false,
        name: "test-system".to_string(),
    };
    let commit_offset: Arc<AtomicI64> = Arc::new(0.into());

    let mut msg_receiver = MessageReceiverService::new(Box::new(mock_receiver), msg_channel_tx, commit_offset, system);
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
