use std::sync::{atomic::AtomicI64, Arc};

use talos_certifier::{
    errors::SystemServiceErrorKind,
    model::{
        candidate_message::CandidateMessage,
        decision_message::{Decision, DecisionMessage},
    },
    ChannelMessage, SystemMessage,
};
use tokio::sync::{broadcast, mpsc};

use crate::{
    core::{DecisionOutboxChannelMessage, System, SystemService},
    services::CertifierService,
};

async fn send_candidate_message(message_channel_tx: mpsc::Sender<ChannelMessage>, candidate_message: CandidateMessage) {
    tokio::spawn(async move {
        message_channel_tx.send(talos_core::ChannelMessage::Candidate(candidate_message)).await.unwrap();
    });
}

struct CertifierChannels {
    system_channel: (broadcast::Sender<SystemMessage>, broadcast::Receiver<SystemMessage>),
    message_channel: (mpsc::Sender<ChannelMessage>, mpsc::Receiver<ChannelMessage>),
    decision_outbox_channel: (mpsc::Sender<DecisionOutboxChannelMessage>, mpsc::Receiver<DecisionOutboxChannelMessage>),
}

async fn get_certifier_channels() -> CertifierChannels {
    CertifierChannels {
        system_channel: broadcast::channel(10),
        message_channel: mpsc::channel(2),
        decision_outbox_channel: mpsc::channel(2),
    }
}

// Successfully process candidate message and certify.
#[tokio::test]
async fn test_certification_rule_2() {
    let channels = get_certifier_channels().await;

    let (do_channel_tx, mut do_channel_rx) = channels.decision_outbox_channel;
    let (message_channel_tx, message_channel_rx) = channels.message_channel;
    let (system_notifier, _system_rx) = channels.system_channel;

    let system = System {
        system_notifier,
        is_shutdown: false,
    };

    let mut certifier_svc = CertifierService::new(message_channel_rx, do_channel_tx, Arc::new(0.into()), system);

    send_candidate_message(
        message_channel_tx.clone(),
        CandidateMessage {
            xid: "xid-1".to_string(),
            version: 8,
            agent: "agent-1".to_string(),
            cohort: "cohort-1".to_string(),
            snapshot: 5,
            readvers: vec![],
            readset: vec![],
            // readvers: vec![3, 4],
            // readset: vec!["ksp:r1".to_owned(), "ksp:r2".to_owned()],
            writeset: vec!["ksp:w1".to_owned()],
            metadata: None,
            on_commit: None,
        },
    )
    .await;
    send_candidate_message(
        message_channel_tx,
        CandidateMessage {
            xid: "xid-1".to_string(),
            version: 10,
            agent: "agent-1".to_string(),
            cohort: "cohort-1".to_string(),
            snapshot: 5,
            readvers: vec![3, 4],
            readset: vec!["ksp:r1".to_owned(), "ksp:r2".to_owned()],
            writeset: vec!["ksp:w1".to_owned()],
            metadata: None,
            on_commit: None,
        },
    )
    .await;

    let _ = certifier_svc.run().await;
    let result = certifier_svc.run().await;

    assert!(result.is_ok());

    // Rule 1
    if let Some(DecisionOutboxChannelMessage::Decision(decision)) = do_channel_rx.recv().await {
        assert!(decision.conflicts.is_none());
        assert_eq!(decision.decision, Decision::Committed);
    };
    // Rule 2
    if let Some(DecisionOutboxChannelMessage::Decision(decision)) = do_channel_rx.recv().await {
        assert!(decision.conflicts.is_none());
        assert_eq!(decision.decision, Decision::Aborted);
    };
}

// ## Error while processing candidate message/certification.
#[tokio::test]
async fn test_error_in_processing_candidate_message_certifying() {
    let channels = get_certifier_channels().await;

    let (do_channel_tx, mut do_channel_rx) = channels.decision_outbox_channel;
    let (message_channel_tx, message_channel_rx) = channels.message_channel;
    let (system_notifier, _system_rx) = channels.system_channel;

    let system = System {
        system_notifier,
        is_shutdown: false,
    };

    let mut certifier_svc = CertifierService::new(message_channel_rx, do_channel_tx, Arc::new(0.into()), system);

    send_candidate_message(
        message_channel_tx.clone(),
        CandidateMessage {
            xid: "xid-1".to_string(),
            version: 8,
            agent: "agent-1".to_string(),
            cohort: "cohort-1".to_string(),
            snapshot: 5,
            readvers: vec![],
            readset: vec![],
            // readvers: vec![3, 4],
            // readset: vec!["ksp:r1".to_owned(), "ksp:r2".to_owned()],
            writeset: vec!["ksp:w1".to_owned()],
            metadata: None,
            on_commit: None,
        },
    )
    .await;

    do_channel_rx.close();

    let result = certifier_svc.run().await;

    assert!(result.is_err());

    if let Err(error) = result {
        assert_eq!(error.kind, SystemServiceErrorKind::SystemError(talos_core::errors::SystemErrorType::Channel));
    }
}

// Process decision message correctly
#[tokio::test]
async fn test_certification_process_decision() {
    let channels = get_certifier_channels().await;

    let (do_channel_tx, mut do_channel_rx) = channels.decision_outbox_channel;
    let (message_channel_tx, message_channel_rx) = channels.message_channel;
    let (system_notifier, _system_rx) = channels.system_channel;

    let system = System {
        system_notifier,
        is_shutdown: false,
    };

    let commit_state: Arc<AtomicI64> = Arc::new(0.into());

    let mut certifier_svc = CertifierService::new(message_channel_rx, do_channel_tx, commit_state.clone(), system);

    let message_channel_tx_clone = message_channel_tx.clone();
    send_candidate_message(
        message_channel_tx_clone,
        CandidateMessage {
            xid: "xid-1".to_string(),
            version: 8,
            agent: "agent-1".to_string(),
            cohort: "cohort-1".to_string(),
            snapshot: 5,
            readvers: vec![],
            readset: vec![],
            // readvers: vec![3, 4],
            // readset: vec!["ksp:r1".to_owned(), "ksp:r2".to_owned()],
            writeset: vec!["ksp:w1".to_owned()],
            metadata: None,
            on_commit: None,
        },
    )
    .await;

    let result = certifier_svc.run().await;

    assert!(result.is_ok());

    if let Some(DecisionOutboxChannelMessage::Decision(decision)) = do_channel_rx.recv().await {
        tokio::spawn(async move {
            message_channel_tx
                .send(talos_core::ChannelMessage::Decision(decision.version, decision))
                .await
                .unwrap();
        });
    };

    let result = certifier_svc.run().await;
    assert!(result.is_ok());

    // ready to commit version 8
    assert_eq!(commit_state.load(std::sync::atomic::Ordering::Relaxed), 8);
}

// Process decision message failed
#[tokio::test]
async fn test_certification_process_decision_incorrect_version() {
    let channels = get_certifier_channels().await;

    let (do_channel_tx, mut do_channel_rx) = channels.decision_outbox_channel;
    let (message_channel_tx, message_channel_rx) = channels.message_channel;
    let (system_notifier, _system_rx) = channels.system_channel;

    let system = System {
        system_notifier,
        is_shutdown: false,
    };

    let mut certifier_svc = CertifierService::new(message_channel_rx, do_channel_tx, Arc::new(0.into()), system);

    let message_channel_tx_clone = message_channel_tx.clone();
    send_candidate_message(
        message_channel_tx_clone,
        CandidateMessage {
            xid: "xid-1".to_string(),
            version: 8,
            agent: "agent-1".to_string(),
            cohort: "cohort-1".to_string(),
            snapshot: 5,
            readvers: vec![],
            readset: vec![],
            // readvers: vec![3, 4],
            // readset: vec!["ksp:r1".to_owned(), "ksp:r2".to_owned()],
            writeset: vec!["ksp:w1".to_owned()],
            metadata: None,
            on_commit: None,
        },
    )
    .await;

    let result = certifier_svc.run().await;
    assert!(result.is_ok());

    // pass incorrect decision (version incorrect), will not do anything as item is not found on suffix.
    if let Some(DecisionOutboxChannelMessage::Decision(decision)) = do_channel_rx.recv().await {
        tokio::spawn(async move {
            message_channel_tx
                .send(talos_core::ChannelMessage::Decision(12, DecisionMessage { version: 10, ..decision }))
                .await
                .unwrap();
        });
    };

    let result = certifier_svc.run().await;
    assert!(result.is_ok());
}
