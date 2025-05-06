use std::time::Duration;

use axum::async_trait;
use serde_json::json;
use tokio::sync::mpsc::{self, error::TryRecvError};

use crate::{
    callbacks::ReplicatorSnapshotProvider,
    services::statemap_queue_service::{self, StatemapQueueServiceConfig},
    StatemapItem, StatemapQueueChannelMessage,
};
pub struct SnapshotApi {
    pub snapshot: u64,
}

#[async_trait]
impl ReplicatorSnapshotProvider for SnapshotApi {
    async fn get_snapshot(&self) -> Result<u64, String> {
        Ok(self.snapshot)
    }
}

#[tokio::test]
async fn test_queue_service_snapshot_version_above_safepoint_below_snapshot() {
    let (statemaps_tx, statemaps_rx) = mpsc::channel(50);
    let (_statemap_installation_tx, statemap_installation_rx) = mpsc::channel(50);
    let (installation_tx, mut installation_rx) = mpsc::channel(50);
    let (replicator_feedback_tx, _) = mpsc::channel(50);
    let config = StatemapQueueServiceConfig {
        queue_cleanup_frequency_ms: 60_000, // 1 hour
        enable_stats: false,
    };
    let snapshot_api = SnapshotApi { snapshot: 8 };

    let mut queue_svc = statemap_queue_service::StatemapQueueService::new(
        statemaps_rx,
        installation_tx,
        statemap_installation_rx,
        replicator_feedback_tx,
        snapshot_api,
        config,
        0,
        None,
    );

    tokio::spawn(async move {
        queue_svc.run().await.unwrap();
    });

    let statemap_item = StatemapItem::new("TestAction".to_string(), 10, json!({"version": 10}), Some(7));
    statemaps_tx
        .send(StatemapQueueChannelMessage::Message((10, vec![statemap_item])))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(10)).await;

    let k = installation_rx.try_recv().unwrap();
    assert_eq!(k.0, 10);
}

/// This test fails. Ideally this should pass.
///
/// snapshot === last version in the contiguous series of items installed.
/// Therefore the only scneario when this could happen is when an inactive replicator becomes active, and it has a very old
/// snapshot value
#[tokio::test]

async fn test_queue_service_version_and_safepoint_above_snapshot() {
    let (statemaps_tx, statemaps_rx) = mpsc::channel(50);
    let (_, statemap_installation_rx) = mpsc::channel(50);
    let (installation_tx, mut installation_rx) = mpsc::channel(50);
    let (replicator_feedback_tx, _) = mpsc::channel(50);
    let config = StatemapQueueServiceConfig {
        queue_cleanup_frequency_ms: 60_000, // 1 hour
        enable_stats: false,
    };
    let snapshot_api = SnapshotApi { snapshot: 8 };

    let mut queue_svc = statemap_queue_service::StatemapQueueService::new(
        statemaps_rx,
        installation_tx,
        statemap_installation_rx,
        replicator_feedback_tx,
        snapshot_api,
        config,
        0,
        None,
    );

    tokio::spawn(async move {
        queue_svc.run().await.unwrap();
    });

    let statemap_item = StatemapItem::new("TestAction".to_string(), 10, json!({"version": 10}), Some(9));
    statemaps_tx
        .send(StatemapQueueChannelMessage::Message((10, vec![statemap_item])))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(10)).await;

    let k = installation_rx.try_recv().unwrap();

    assert_eq!(k.0, 10);
}

/// This test fails. Ideally this should pass.
///
/// Should not pick anything and therefore should not receive anything.
/// If version is below, we shouldn't even insert into the queue.
#[tokio::test]
async fn test_queue_service_version_and_safepoint_below_snapshot() {
    let (statemaps_tx, statemaps_rx) = mpsc::channel(50);
    let (_, statemap_installation_rx) = mpsc::channel(50);
    let (installation_tx, mut installation_rx) = mpsc::channel(50);
    let (replicator_feedback_tx, _) = mpsc::channel(50);
    let config = StatemapQueueServiceConfig {
        queue_cleanup_frequency_ms: 60_000, // 1 hour
        enable_stats: false,
    };
    let snapshot_api = SnapshotApi { snapshot: 8 };

    let mut queue_svc = statemap_queue_service::StatemapQueueService::new(
        statemaps_rx,
        installation_tx,
        statemap_installation_rx,
        replicator_feedback_tx,
        snapshot_api,
        config,
        0,
        None,
    );

    tokio::spawn(async move {
        queue_svc.run().await.unwrap();
    });

    let statemap_item = StatemapItem::new("TestAction".to_string(), 7, json!({"version": 7}), Some(5));
    statemaps_tx.send(StatemapQueueChannelMessage::Message((7, vec![statemap_item]))).await.unwrap();

    tokio::time::sleep(Duration::from_millis(10)).await;

    let receive_result = installation_rx.try_recv();
    assert_eq!(receive_result, Err(TryRecvError::Empty));
}
