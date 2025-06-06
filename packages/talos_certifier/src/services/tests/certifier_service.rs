use std::sync::{atomic::AtomicI64, Arc};

use crate::{
    core::{CandidateChannelMessage, DecisionChannelMessage},
    errors::{SystemErrorType, SystemServiceErrorKind},
    model::{
        CandidateMessage, {Decision, DecisionMessage},
    },
    services::CertifierServiceConfig,
    ChannelMessage, SystemMessage,
};
use ahash::{HashMap, HashMapExt};
use talos_suffix::core::SuffixConfig;
use tokio::sync::{broadcast, mpsc};

use crate::{
    core::{DecisionOutboxChannelMessage, System, SystemService},
    services::CertifierService,
};

async fn send_candidate_message(message_channel_tx: mpsc::Sender<ChannelMessage<CandidateMessage>>, candidate_message: CandidateMessage) {
    tokio::spawn(async move {
        message_channel_tx
            .send(ChannelMessage::Candidate(
                CandidateChannelMessage {
                    message: candidate_message,
                    headers: HashMap::new(),
                }
                .into(),
            ))
            .await
            .unwrap();
    });
}

struct CertifierChannels {
    system_channel: (broadcast::Sender<SystemMessage>, broadcast::Receiver<SystemMessage>),
    message_channel: (mpsc::Sender<ChannelMessage<CandidateMessage>>, mpsc::Receiver<ChannelMessage<CandidateMessage>>),
    decision_outbox_channel: (mpsc::Sender<DecisionOutboxChannelMessage>, mpsc::Receiver<DecisionOutboxChannelMessage>),
}

async fn get_certifier_channels() -> CertifierChannels {
    CertifierChannels {
        system_channel: broadcast::channel(10),
        message_channel: mpsc::channel(5),
        decision_outbox_channel: mpsc::channel(5),
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
        name: "test-system".to_string(),
    };

    let mut certifier_svc = CertifierService::new(message_channel_rx, do_channel_tx, Arc::new(0.into()), system, None);

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
            certification_started_at: 0,
            request_created_at: 0,
            published_at: 0,
            received_at: 0,
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
            certification_started_at: 0,
            request_created_at: 0,
            published_at: 0,
            received_at: 0,
        },
    )
    .await;

    let _ = certifier_svc.run().await;
    let result = certifier_svc.run().await;

    assert!(result.is_ok());

    // Rule 1
    if let Some(decision) = do_channel_rx.recv().await {
        assert!(decision.message.conflict_version.is_none());
        assert_eq!(decision.message.decision, Decision::Committed);
    };
    // Rule 2
    if let Some(decision) = do_channel_rx.recv().await {
        assert!(decision.message.conflict_version.is_none());
        assert_eq!(decision.message.decision, Decision::Aborted);
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
        name: "test-system".to_string(),
    };

    let mut certifier_svc = CertifierService::new(message_channel_rx, do_channel_tx, Arc::new(0.into()), system, None);

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
            certification_started_at: 0,
            request_created_at: 0,
            published_at: 0,
            received_at: 0,
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
    let (system_notifier, _system_rx) = channels.system_channel;

    let system = System {
        system_notifier,
        is_shutdown: false,
        name: "test-system".to_string(),
    };

    let commit_state: Arc<AtomicI64> = Arc::new(0.into());

    let mut certifier_svc = CertifierService::new(message_channel_rx, do_channel_tx, commit_state.clone(), system, None);

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
            certification_started_at: 0,
            request_created_at: 0,
            published_at: 0,
            received_at: 0,
        },
    )
    .await;

    let result = certifier_svc.run().await;

    assert!(result.is_ok());

    if let Some(decision) = do_channel_rx.recv().await {
        tokio::spawn(async move {
            message_channel_tx
                .send(ChannelMessage::Decision(
                    DecisionChannelMessage {
                        decision_version: decision.message.version,
                        message: decision.message,
                        headers: HashMap::new(),
                    }
                    .into(),
                ))
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
        name: "test-system".to_string(),
    };

    let mut certifier_svc = CertifierService::new(message_channel_rx, do_channel_tx, Arc::new(0.into()), system, None);

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
            certification_started_at: 0,
            request_created_at: 0,
            published_at: 0,
            received_at: 0,
        },
    )
    .await;

    let result = certifier_svc.run().await;
    assert!(result.is_ok());

    // pass incorrect decision (version incorrect), will not do anything as item is not found on suffix.
    if let Some(decision) = do_channel_rx.recv().await {
        tokio::spawn(async move {
            message_channel_tx
                .send(ChannelMessage::Decision(
                    DecisionChannelMessage {
                        decision_version: 12,
                        message: DecisionMessage {
                            version: 10,
                            ..decision.message
                        },
                        headers: HashMap::new(),
                    }
                    .into(),
                ))
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
    let (system_notifier, _system_rx) = channels.system_channel;

    let system = System {
        system_notifier,
        is_shutdown: false,
        name: "test-system".to_string(),
    };

    let mut certifier_svc = CertifierService::new(
        message_channel_rx,
        do_channel_tx,
        Arc::new(0.into()),
        system,
        Some(CertifierServiceConfig {
            suffix_config: SuffixConfig {
                capacity: 5,
                prune_start_threshold: Some(3),
                min_size_after_prune: Some(2),
            },
            otel_grpc_endpoint: None,
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
            certification_started_at: 0,
            request_created_at: 0,
            published_at: 0,
            received_at: 0,
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
            certification_started_at: 0,
            request_created_at: 0,
            published_at: 0,
            received_at: 0,
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
            certification_started_at: 0,
            request_created_at: 0,
            published_at: 0,
            received_at: 0,
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
            certification_started_at: 0,
            request_created_at: 0,
            published_at: 0,
            received_at: 0,
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
            certification_started_at: 0,
            request_created_at: 0,
            published_at: 0,
            received_at: 0,
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

    if let Some(decision) = do_channel_rx.recv().await {
        tokio::spawn({
            let message_channel_tx_clone = message_channel_tx.clone();
            async move {
                let _ = message_channel_tx_clone
                    .send(ChannelMessage::Decision(
                        DecisionChannelMessage {
                            decision_version: 6,
                            message: decision.message,
                            headers: HashMap::new(),
                        }
                        .into(),
                    ))
                    .await;
            }
        });
    };
    if let Some(decision) = do_channel_rx.recv().await {
        tokio::spawn({
            let message_channel_tx_clone = message_channel_tx.clone();
            async move {
                let _ = message_channel_tx_clone
                    .send(ChannelMessage::Decision(
                        DecisionChannelMessage {
                            decision_version: 7,
                            message: decision.message,
                            headers: HashMap::new(),
                        }
                        .into(),
                    ))
                    .await;
            }
        });
    };
    if let Some(decision) = do_channel_rx.recv().await {
        tokio::spawn({
            let message_channel_tx_clone = message_channel_tx.clone();
            async move {
                let _ = message_channel_tx_clone
                    .send(ChannelMessage::Decision(
                        DecisionChannelMessage {
                            decision_version: 8,
                            message: decision.message,
                            headers: HashMap::new(),
                        }
                        .into(),
                    ))
                    .await;
            }
        });
    };
    if let Some(decision) = do_channel_rx.recv().await {
        tokio::spawn({
            let message_channel_tx_clone = message_channel_tx.clone();
            async move {
                let _ = message_channel_tx_clone
                    .send(ChannelMessage::Decision(
                        DecisionChannelMessage {
                            decision_version: 10,
                            message: decision.message,
                            headers: HashMap::new(),
                        }
                        .into(),
                    ))
                    .await;
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
    let (system_notifier, _system_rx) = channels.system_channel;

    let system = System {
        system_notifier,
        is_shutdown: false,
        name: "test-system".to_string(),
    };

    let mut certifier_svc = CertifierService::new(
        message_channel_rx,
        do_channel_tx,
        Arc::new(0.into()),
        system,
        Some(CertifierServiceConfig {
            suffix_config: SuffixConfig {
                capacity: 5,
                ..Default::default()
            },
            otel_grpc_endpoint: None,
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
            certification_started_at: 0,
            request_created_at: 0,
            published_at: 0,
            received_at: 0,
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
            certification_started_at: 0,
            request_created_at: 0,
            published_at: 0,
            received_at: 0,
        },
    )
    .await;

    // certifier service processing candidate messages
    let _ = certifier_svc.run().await;
    let result = certifier_svc.run().await;
    assert!(result.is_ok());

    if let Some(decision) = do_channel_rx.recv().await {
        tokio::spawn({
            let message_channel_tx_clone = message_channel_tx.clone();
            async move {
                let _ = message_channel_tx_clone
                    .send(ChannelMessage::Decision(
                        DecisionChannelMessage {
                            decision_version: 6,
                            message: decision.message,
                            headers: HashMap::new(),
                        }
                        .into(),
                    ))
                    .await;
            }
        });
    };
    if let Some(decision) = do_channel_rx.recv().await {
        tokio::spawn({
            let message_channel_tx_clone = message_channel_tx.clone();
            async move {
                let _ = message_channel_tx_clone
                    .send(ChannelMessage::Decision(
                        DecisionChannelMessage {
                            decision_version: 7,
                            message: decision.message,
                            headers: HashMap::new(),
                        }
                        .into(),
                    ))
                    .await;
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
