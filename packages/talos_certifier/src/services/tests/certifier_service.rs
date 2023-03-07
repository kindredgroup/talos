use std::sync::{atomic::AtomicI64, Arc};

use crate::{
    errors::{SystemErrorType, SystemServiceErrorKind},
    model::{
        CandidateMessage, {Decision, DecisionMessage},
    },
    services::CertifierServiceConfig,
    ChannelMessage, SystemMessage,
};
use talos_suffix::core::SuffixConfig;
use tokio::sync::{broadcast, mpsc};

use crate::{
    core::{DecisionOutboxChannelMessage, System, SystemService},
    services::CertifierService,
};

async fn send_candidate_message(message_channel_tx: mpsc::Sender<ChannelMessage>, candidate_message: CandidateMessage) {
    tokio::spawn(async move {
        message_channel_tx.send(ChannelMessage::Candidate(candidate_message)).await.unwrap();
    });
}

struct CertifierChannels {
    system_channel: (broadcast::Sender<SystemMessage>, broadcast::Receiver<SystemMessage>),
    message_channel: (mpsc::Sender<ChannelMessage>, mpsc::Receiver<ChannelMessage>),
    decision_message_channel: (mpsc::Sender<ChannelMessage>, mpsc::Receiver<ChannelMessage>),
    decision_outbox_channel: (mpsc::Sender<DecisionOutboxChannelMessage>, mpsc::Receiver<DecisionOutboxChannelMessage>),
}

async fn get_certifier_channels() -> CertifierChannels {
    CertifierChannels {
        system_channel: broadcast::channel(10),
        message_channel: mpsc::channel(5),
        decision_message_channel: mpsc::channel(5),
        decision_outbox_channel: mpsc::channel(5),
    }
}

// Successfully process candidate message and certify.
#[tokio::test]
async fn test_certification_rule_2() {
    let channels = get_certifier_channels().await;

    let (do_channel_tx, mut do_channel_rx) = channels.decision_outbox_channel;
    let (message_channel_tx, message_channel_rx) = channels.message_channel;
    let (_decision_message_channel_tx, decision_message_channel_rx) = channels.decision_message_channel;
    let (system_notifier, _system_rx) = channels.system_channel;

    let system = System {
        system_notifier,
        is_shutdown: false,
    };

    let mut certifier_svc = CertifierService::new(message_channel_rx, decision_message_channel_rx, do_channel_tx, Arc::new(0.into()), system, None);

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
    let (_decision_message_channel_tx, decision_message_channel_rx) = channels.decision_message_channel;
    let (system_notifier, _system_rx) = channels.system_channel;

    let system = System {
        system_notifier,
        is_shutdown: false,
    };

    let mut certifier_svc = CertifierService::new(message_channel_rx, decision_message_channel_rx, do_channel_tx, Arc::new(0.into()), system, None);

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
        assert_eq!(error.kind, SystemServiceErrorKind::SystemError(SystemErrorType::Channel));
    }
}

// Process decision message correctly
#[tokio::test]
async fn test_certification_process_decision() {
    let channels = get_certifier_channels().await;

    let (do_channel_tx, mut do_channel_rx) = channels.decision_outbox_channel;
    let (message_channel_tx, message_channel_rx) = channels.message_channel;
    let (decision_message_channel_tx, decision_message_channel_rx) = channels.decision_message_channel;
    let (system_notifier, _system_rx) = channels.system_channel;

    let system = System {
        system_notifier,
        is_shutdown: false,
    };

    let commit_state: Arc<AtomicI64> = Arc::new(0.into());

    let mut certifier_svc = CertifierService::new(
        message_channel_rx,
        decision_message_channel_rx,
        do_channel_tx,
        commit_state.clone(),
        system,
        None,
    );

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
            decision_message_channel_tx
                .send(ChannelMessage::Decision(decision.version, decision))
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
    let (decision_message_channel_tx, decision_message_channel_rx) = channels.decision_message_channel;
    let (system_notifier, _system_rx) = channels.system_channel;

    let system = System {
        system_notifier,
        is_shutdown: false,
    };

    let mut certifier_svc = CertifierService::new(message_channel_rx, decision_message_channel_rx, do_channel_tx, Arc::new(0.into()), system, None);

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
            decision_message_channel_tx
                .send(ChannelMessage::Decision(12, DecisionMessage { version: 10, ..decision }))
                .await
                .unwrap();
        });
    };

    let result = certifier_svc.run().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_certification_check_suffix_prune_is_ready_threshold_30pc() {
    let channels = get_certifier_channels().await;

    let (do_channel_tx, mut do_channel_rx) = channels.decision_outbox_channel;
    let (message_channel_tx, message_channel_rx) = channels.message_channel;
    let (decision_message_channel_tx, decision_message_channel_rx) = channels.decision_message_channel;
    let (system_notifier, _system_rx) = channels.system_channel;

    let system = System {
        system_notifier,
        is_shutdown: false,
    };

    let mut certifier_svc = CertifierService::new(
        message_channel_rx,
        decision_message_channel_rx,
        do_channel_tx,
        Arc::new(0.into()),
        system,
        Some(CertifierServiceConfig {
            suffix_config: SuffixConfig {
                capacity: 5,
                prune_start_threshold: Some(3),
                min_size_after_prune: Some(2),
            },
        }),
    );

    let message_channel_tx_clone1 = message_channel_tx.clone();
    let message_channel_tx_clone2 = message_channel_tx.clone();
    let message_channel_tx_clone3 = message_channel_tx.clone();
    let message_channel_tx_clone4 = message_channel_tx.clone();
    let message_channel_tx_clone5 = message_channel_tx.clone();
    send_candidate_message(
        message_channel_tx_clone1,
        CandidateMessage {
            xid: "xid-1".to_string(),
            version: 1,
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
        message_channel_tx_clone2,
        CandidateMessage {
            xid: "xid-2".to_string(),
            version: 2,
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
        message_channel_tx_clone3,
        CandidateMessage {
            xid: "xid-3".to_string(),
            version: 3,
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
        message_channel_tx_clone4,
        CandidateMessage {
            xid: "xid-4".to_string(),
            version: 4,
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
        message_channel_tx_clone5,
        CandidateMessage {
            xid: "xid-5".to_string(),
            version: 5,
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

    // certifier service processing candidate messages
    let _ = certifier_svc.run().await;
    let _ = certifier_svc.run().await;
    let result = certifier_svc.run().await;
    assert!(result.is_ok());
    let result = certifier_svc.run().await;
    assert!(result.is_ok());
    let result = certifier_svc.run().await;
    assert!(result.is_ok());

    if let Some(DecisionOutboxChannelMessage::Decision(decision)) = do_channel_rx.recv().await {
        tokio::spawn({
            let decision_message_channel_tx_clone = decision_message_channel_tx.clone();
            async move {
                decision_message_channel_tx_clone
                    .send(ChannelMessage::Decision(6, DecisionMessage { ..decision }))
                    .await
                    .unwrap();
            }
        });
    };
    if let Some(DecisionOutboxChannelMessage::Decision(decision)) = do_channel_rx.recv().await {
        tokio::spawn({
            let decision_message_channel_tx_clone = decision_message_channel_tx.clone();

            async move {
                decision_message_channel_tx_clone
                    .send(ChannelMessage::Decision(7, DecisionMessage { ..decision }))
                    .await
                    .unwrap();
            }
        });
    };
    if let Some(DecisionOutboxChannelMessage::Decision(decision)) = do_channel_rx.recv().await {
        tokio::spawn({
            let decision_message_channel_tx_clone = decision_message_channel_tx.clone();

            async move {
                decision_message_channel_tx_clone
                    .send(ChannelMessage::Decision(8, DecisionMessage { ..decision }))
                    .await
                    .unwrap();
            }
        });
    };
    if let Some(DecisionOutboxChannelMessage::Decision(decision)) = do_channel_rx.recv().await {
        tokio::spawn({
            let decision_message_channel_tx_clone = decision_message_channel_tx.clone();

            async move {
                decision_message_channel_tx_clone
                    .send(ChannelMessage::Decision(10, DecisionMessage { ..decision }))
                    .await
                    .unwrap();
            }
        });
    };

    // certifier service processing the decisions
    let _ = certifier_svc.run().await;
    let prune_index = certifier_svc.suffix.meta.prune_index;
    assert!(prune_index.is_some());

    let _ = certifier_svc.run().await;
    let _ = certifier_svc.run().await;
    let result = certifier_svc.run().await;
    assert!(result.is_ok());

    // let prune_index = certifier_svc.suffix.meta.prune_index;
    // assert!(prune_index.is_some());

    assert!(certifier_svc.suffix.get_safe_prune_index().is_none());
}

#[tokio::test]
async fn test_certification_check_suffix_prune_is_not_at_threshold() {
    let channels = get_certifier_channels().await;

    let (do_channel_tx, mut do_channel_rx) = channels.decision_outbox_channel;
    let (message_channel_tx, message_channel_rx) = channels.message_channel;
    let (decision_message_channel_tx, decision_message_channel_rx) = channels.decision_message_channel;
    let (system_notifier, _system_rx) = channels.system_channel;

    let system = System {
        system_notifier,
        is_shutdown: false,
    };

    let mut certifier_svc = CertifierService::new(
        message_channel_rx,
        decision_message_channel_rx,
        do_channel_tx,
        Arc::new(0.into()),
        system,
        Some(CertifierServiceConfig {
            suffix_config: SuffixConfig {
                capacity: 5,
                ..Default::default()
            },
        }),
    );

    let message_channel_tx_clone1 = message_channel_tx.clone();
    let message_channel_tx_clone2 = message_channel_tx.clone();
    send_candidate_message(
        message_channel_tx_clone1,
        CandidateMessage {
            xid: "xid-1".to_string(),
            version: 1,
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
        message_channel_tx_clone2,
        CandidateMessage {
            xid: "xid-1".to_string(),
            version: 2,
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

    // certifier service processing candidate messages
    let _ = certifier_svc.run().await;
    let result = certifier_svc.run().await;
    assert!(result.is_ok());

    if let Some(DecisionOutboxChannelMessage::Decision(decision)) = do_channel_rx.recv().await {
        tokio::spawn({
            let decision_message_channel_tx_clone = decision_message_channel_tx.clone();
            async move {
                decision_message_channel_tx_clone
                    .send(ChannelMessage::Decision(6, DecisionMessage { ..decision }))
                    .await
                    .unwrap();
            }
        });
    };
    if let Some(DecisionOutboxChannelMessage::Decision(decision)) = do_channel_rx.recv().await {
        tokio::spawn({
            let decision_message_channel_tx_clone = decision_message_channel_tx.clone();

            async move {
                decision_message_channel_tx_clone
                    .send(ChannelMessage::Decision(7, DecisionMessage { ..decision }))
                    .await
                    .unwrap();
            }
        });
    };

    // certifier service processing the decisions
    let _ = certifier_svc.run().await;
    let result = certifier_svc.run().await;
    assert!(result.is_ok());

    // assert suffix is above 50pc and prune ready
    assert_eq!(certifier_svc.suffix.meta.prune_index, Some(1));
    assert!(certifier_svc.suffix.get_safe_prune_index().is_none());
}
