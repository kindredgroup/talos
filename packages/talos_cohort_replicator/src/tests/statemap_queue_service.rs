use std::{
    collections::{HashSet, VecDeque},
    time::Duration,
};

use axum::async_trait;
use serde_json::json;
use tokio::sync::mpsc::{self, error::TryRecvError};

use crate::{
    callbacks::ReplicatorSnapshotProvider,
    services::statemap_queue_service::{self, StatemapQueueServiceConfig},
    StatemapItem, StatemapQueueChannelMessage,
};

#[derive(Default)]
pub struct MockSnapshotApi {
    pub snapshot: u64,
    pub next_snapshots: VecDeque<u64>,
}

#[async_trait]
impl ReplicatorSnapshotProvider for MockSnapshotApi {
    async fn get_snapshot(&self) -> Result<u64, String> {
        let snapshot = if !self.next_snapshots.is_empty() {
            *self.next_snapshots.front().unwrap()
        } else {
            self.snapshot
        };
        Ok(snapshot)
    }

    async fn update_snapshot(&self, _version: u64) -> Result<(), String> {
        Ok(())
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
    let snapshot_api = MockSnapshotApi {
        snapshot: 8,
        ..Default::default()
    };

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
    let snapshot_api = MockSnapshotApi {
        snapshot: 8,
        ..Default::default()
    };

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

// Test covers below possible scenarios that can impact the snapshot_version or items and their states in the queue
// - Test Receiving and inserting message to queue
//      - when version received is less than the snapshot version.
//      - when version received is greater than the snapshot version.
// - Test picking of statemaps for installation
//      - Check against `StatemapInstallState`
//      - Check safepoint against snapshot version
//      - Check safepoint to see which items are serialisable.
// - Test feedback from install StatemapInstallationStatus::Success(version)
//      - When all version below are successfully installed.
//      - When there are some versions not installed yet.
// - Test feedback from install StatemapInstallationStatus::Error
//      - Check when item is moved back to `Awaiting`, how does it behave if some other version has this version as safepoint
//      - Check when item is moved back to `Awaiting`, how does it behave if some other version doesn't have this version as safepoint
#[tokio::test]
async fn test_queue_service_insert_install_feedbacks() {
    let (statemaps_tx, statemaps_rx) = mpsc::channel(50);
    let (installation_feedback_tx, installation_feedback_rx) = mpsc::channel(50);
    let (installation_tx, mut installation_rx) = mpsc::channel(50);
    let (replicator_feedback_tx, _) = mpsc::channel(50);
    let config = StatemapQueueServiceConfig {
        queue_cleanup_frequency_ms: 60_000, // 1 hour
        enable_stats: false,
    };

    let initial_snapshot_version = 15;
    let snapshot_api = MockSnapshotApi {
        snapshot: initial_snapshot_version,
        ..Default::default()
    };

    let mut queue_svc = statemap_queue_service::StatemapQueueService::new(
        statemaps_rx,
        installation_tx,
        installation_feedback_rx,
        replicator_feedback_tx,
        snapshot_api,
        config,
        0,
        None,
    );

    // set the snapshot for queue_service
    queue_svc.statemap_queue.snapshot_version = initial_snapshot_version;

    //*****/
    let install_vers = 7;
    // Some safepoint = Committed outcome decision for the candidate message.
    let safepoint = Some(5);
    let statemap_item = StatemapItem::new(
        "TestAction".to_string(),
        install_vers,
        json!({"mockPayload": format!("some mock data {install_vers}")}),
        safepoint,
    );
    statemaps_tx
        .send(StatemapQueueChannelMessage::Message((install_vers, vec![statemap_item])))
        .await
        .unwrap();

    //*****/
    let install_vers = 8;
    // None safepoint = Abort outcome decision for the candidate message.
    let safepoint = None;
    let statemap_item = StatemapItem::new(
        "TestAction".to_string(),
        install_vers,
        json!({"mockPayload": format!("some mock data {install_vers}")}),
        safepoint,
    );
    statemaps_tx
        .send(StatemapQueueChannelMessage::Message((install_vers, vec![statemap_item])))
        .await
        .unwrap();

    // Run the service tokio::select arm once
    queue_svc.run_once().await.unwrap();
    // Run the service tokio::select arm once
    queue_svc.run_once().await.unwrap();

    // install version 7 and 8 are below the snapshot_version, hence it is marked as installed and nothing is send for installation.
    // Therefore, we will get empty
    let receive_result = installation_rx.try_recv();
    assert_eq!(receive_result, Err(TryRecvError::Empty));

    //*****/
    // install version is > snapshot_version and safepoint is None, hence this will be picked up
    //*****/
    let install_vers = 16;
    // None safepoint = Abort outcome decision for the candidate message.
    let safepoint = None;
    let statemap_item = StatemapItem::new(
        "TestAction".to_string(),
        install_vers,
        json!({"mockPayload": format!("some mock data {install_vers}")}),
        safepoint,
    );
    statemaps_tx
        .send(StatemapQueueChannelMessage::Message((install_vers, vec![statemap_item])))
        .await
        .unwrap();

    //*****/
    // install version is > snapshot_version and safepoint < snapshot_version and not in the queue, hence it will be picked up
    //*****/
    let install_vers = 17;
    let safepoint = Some(2);
    let statemap_item = StatemapItem::new(
        "TestAction".to_string(),
        install_vers,
        json!({"mockPayload": format!("some mock data {install_vers}")}),
        safepoint,
    );
    statemaps_tx
        .send(StatemapQueueChannelMessage::Message((install_vers, vec![statemap_item])))
        .await
        .unwrap();

    //*****/
    // install version is > snapshot_version and safepoint < snapshot_version, and safepoint point version in queue and installed, hence it will be picked up
    //*****/
    let install_vers = 18;
    let safepoint = Some(2);
    let statemap_item = StatemapItem::new(
        "TestAction".to_string(),
        install_vers,
        json!({"mockPayload": format!("some mock data {install_vers}")}),
        safepoint,
    );
    statemaps_tx
        .send(StatemapQueueChannelMessage::Message((install_vers, vec![statemap_item])))
        .await
        .unwrap();

    //*****/
    // install version is > snapshot_version and safepoint > snapshot_version, hence it will not be picked up for installation.
    //*****/
    let install_vers = 19;
    let safepoint = Some(16);
    let statemap_item = StatemapItem::new(
        "TestAction".to_string(),
        install_vers,
        json!({"mockPayload": format!("some mock data {install_vers}")}),
        safepoint,
    );
    statemaps_tx
        .send(StatemapQueueChannelMessage::Message((install_vers, vec![statemap_item])))
        .await
        .unwrap();

    // Run the service tokio::select arm once
    queue_svc.run_once().await.unwrap();
    // Run the service tokio::select arm once
    queue_svc.run_once().await.unwrap();
    // Run the service tokio::select arm once
    queue_svc.run_once().await.unwrap();
    // Run the service tokio::select arm once
    queue_svc.run_once().await.unwrap();

    // Snapshot still at the initial version
    assert_eq!(queue_svc.statemap_queue.snapshot_version, initial_snapshot_version);
    // Although we inserted 4 new versions, version 19 has safepoint 16,
    // hence it will not be picked for installation.
    // Therefore only 3 items will be in the channel for installation.
    assert_eq!(installation_rx.max_capacity() - installation_rx.capacity(), 3);

    {
        // Retrieve the versions
        let _ = installation_rx.try_recv().unwrap();
        let _ = installation_rx.try_recv().unwrap();
        let _ = installation_rx.try_recv().unwrap();
    }

    // Success feedback for version 18
    installation_feedback_tx
        .send(crate::core::StatemapInstallationStatus::Success(18))
        .await
        .unwrap();

    // Run the service tokio::select to process the feedback arm.
    queue_svc.run_once().await.unwrap();
    assert_eq!(queue_svc.statemap_queue.snapshot_version, initial_snapshot_version);

    // Success feedback for version 16
    installation_feedback_tx
        .send(crate::core::StatemapInstallationStatus::Success(16))
        .await
        .unwrap();
    // Run the service tokio::select to process the feedback arm.
    queue_svc.run_once().await.unwrap();

    // version 19 was picked for installation now, as snapshot has moved to 16
    assert_eq!(installation_rx.max_capacity() - installation_rx.capacity(), 1);
    assert_eq!(queue_svc.statemap_queue.snapshot_version, 16);

    // Error feedback for version 17
    installation_feedback_tx
        .send(crate::core::StatemapInstallationStatus::Error(17, "Error".to_string()))
        .await
        .unwrap();

    // Run the service tokio::select to process the feedback arm.
    queue_svc.run_once().await.unwrap();
    assert_eq!(installation_rx.max_capacity() - installation_rx.capacity(), 2);

    //*****/
    // install version is > snapshot_version and safepoint > snapshot_version, hence it will not be picked up for installation.
    //*****/
    let install_vers = 20;
    let safepoint = Some(17);
    let statemap_item = StatemapItem::new(
        "TestAction".to_string(),
        install_vers,
        json!({"mockPayload": format!("some mock data {install_vers}")}),
        safepoint,
    );
    statemaps_tx
        .send(StatemapQueueChannelMessage::Message((install_vers, vec![statemap_item])))
        .await
        .unwrap();
    // Run the service to insert this into queue.
    queue_svc.run_once().await.unwrap();
    // Version 20 will not be picked for installation as the safepoint version 17 is not installed yet.
    assert_eq!(installation_rx.max_capacity() - installation_rx.capacity(), 2);

    //##############################################################################################################
    //
    //                           F I N A L    A S S E R T I O N S
    //
    //--------------------------------------------------------------------------------------------------------------
    // Checks to see after running through series of statemaps to install, everything is as expected on the queue
    // and with snapshot_version
    //##############################################################################################################
    // snapshot_version = 16 | Total of 7 items in the queue
    assert_eq!(queue_svc.statemap_queue.snapshot_version, 16);
    // Version 7,8,16 and 18 are in `installed` state.
    let installed_versions = queue_svc
        .statemap_queue
        .filter_items_by_state(crate::core::StatemapInstallState::Installed)
        .map(|x| x.version)
        .collect::<HashSet<u64>>();
    assert!(installed_versions.contains(&7));
    assert!(installed_versions.contains(&8));
    assert!(installed_versions.contains(&16));
    assert!(installed_versions.contains(&18));
    // Version 17 and 19 are in `inflight` state.
    let inflight_versions = queue_svc
        .statemap_queue
        .filter_items_by_state(crate::core::StatemapInstallState::Inflight)
        .map(|x| x.version)
        .collect::<HashSet<u64>>();
    assert!(inflight_versions.contains(&17));
    assert!(inflight_versions.contains(&19));
    // Version 20 is in awaiting state
    let awaiting_versions = queue_svc
        .statemap_queue
        .filter_items_by_state(crate::core::StatemapInstallState::Awaiting)
        .map(|x| x.version)
        .collect::<HashSet<u64>>();
    assert!(awaiting_versions.contains(&20));
}

// Test covers below possible scenarios that can impact the snapshot_version or items and their states in the queue
// - Test Resetting the snapshot_version using callback
//      - Test when version is not on queue, get the nearest one to use for pruning.
//      - Test when new snapshot >= internal snapshot_version
//      - Test when new snapshot < internal snapshot_version
// - Test statemap_queue prune logic.
#[tokio::test]
async fn test_queue_service_effects_of_resetting_snapshot() {
    let (statemaps_tx, statemaps_rx) = mpsc::channel(50);
    let (installation_feedback_tx, installation_feedback_rx) = mpsc::channel(50);
    let (installation_tx, mut installation_rx) = mpsc::channel(50);
    let (replicator_feedback_tx, _) = mpsc::channel(50);
    let config = StatemapQueueServiceConfig {
        queue_cleanup_frequency_ms: 60_000, // 1 hour
        enable_stats: false,
    };

    let initial_snapshot_version = 10;
    let next_snapshots: Vec<u64> = vec![19];
    let snapshot_api = MockSnapshotApi {
        snapshot: initial_snapshot_version,
        next_snapshots: next_snapshots.into(),
    };

    let mut queue_svc = statemap_queue_service::StatemapQueueService::new(
        statemaps_rx,
        installation_tx,
        installation_feedback_rx,
        replicator_feedback_tx,
        snapshot_api,
        config,
        0,
        None,
    );

    // set the snapshot for queue_service
    queue_svc.statemap_queue.snapshot_version = initial_snapshot_version;

    //*****/
    // install version is > snapshot_version and safepoint < snapshot_version, hence it will be picked up for installation.
    //*****/
    let install_vers = 12;
    let safepoint = Some(2);
    let statemap_item = StatemapItem::new(
        "TestAction".to_string(),
        install_vers,
        json!({"mockPayload": format!("some mock data {install_vers}")}),
        safepoint,
    );
    statemaps_tx
        .send(StatemapQueueChannelMessage::Message((install_vers, vec![statemap_item])))
        .await
        .unwrap();
    // Run the service to insert this into queue.
    queue_svc.run_once().await.unwrap();
    //*****/
    // install version is > snapshot_version and safepoint > snapshot_version, hence it will not be picked up for installation.
    //*****/
    let install_vers = 13;
    let safepoint = Some(12);
    let statemap_item = StatemapItem::new(
        "TestAction".to_string(),
        install_vers,
        json!({"mockPayload": format!("some mock data {install_vers}")}),
        safepoint,
    );
    statemaps_tx
        .send(StatemapQueueChannelMessage::Message((install_vers, vec![statemap_item])))
        .await
        .unwrap();
    // Run the service to insert this into queue.
    queue_svc.run_once().await.unwrap();
    //*****/
    // install version is > snapshot_version and safepoint is not available as it was abort decision. So although this is eligible to be picked, it will not as the version prioir to this has safepoint constraint.
    //*****/
    let install_vers = 15;
    let safepoint = None;
    let statemap_item = StatemapItem::new(
        "TestAction".to_string(),
        install_vers,
        json!({"mockPayload": format!("some mock data {install_vers}")}),
        safepoint,
    );
    statemaps_tx
        .send(StatemapQueueChannelMessage::Message((install_vers, vec![statemap_item])))
        .await
        .unwrap();
    // Run the service to insert this into queue.
    queue_svc.run_once().await.unwrap();

    assert_eq!(installation_rx.max_capacity() - installation_rx.capacity(), 1);
    assert_eq!(queue_svc.statemap_queue.snapshot_version, initial_snapshot_version);

    let res = installation_rx.try_recv().unwrap();
    // Success feedback for version 12
    installation_feedback_tx
        .send(crate::core::StatemapInstallationStatus::Success(res.0))
        .await
        .unwrap();
    // Run the service to process the feedback from version 12.
    queue_svc.run_once().await.unwrap();

    assert_eq!(installation_rx.max_capacity() - installation_rx.capacity(), 2);
    assert_eq!(queue_svc.statemap_queue.snapshot_version, 12);

    //*****/
    // install version is > snapshot_version and safepoint > snapshot_version, hence it will not be picked.
    //*****/
    let install_vers = 20;
    let safepoint = Some(18);
    let statemap_item = StatemapItem::new(
        "TestAction".to_string(),
        install_vers,
        json!({"mockPayload": format!("some mock data {install_vers}")}),
        safepoint,
    );
    statemaps_tx
        .send(StatemapQueueChannelMessage::Message((install_vers, vec![statemap_item])))
        .await
        .unwrap();
    // Run the service to insert this into queue.
    queue_svc.run_once().await.unwrap();
    assert_eq!(installation_rx.max_capacity() - installation_rx.capacity(), 2);

    // We have inserted 4 items so length should be 4.
    assert_eq!(queue_svc.statemap_queue.queue.len(), 4);

    // Get next snapshot
    statemaps_tx.send(StatemapQueueChannelMessage::UpdateSnapshot).await.unwrap();

    // Run the service to update the snapshot + prune all items in queue below the snapshot_version.
    queue_svc.run_once().await.unwrap();

    // Only version 20 is above the snapshot, everything else can be removed.
    assert_eq!(queue_svc.statemap_queue.queue.len(), 1);
    assert_eq!(queue_svc.statemap_queue.snapshot_version, 19);

    // The two items send for install already + version 20 is send now.
    assert_eq!(installation_rx.max_capacity() - installation_rx.capacity(), 3);

    // Send feedback for version 13
    let res = installation_rx.try_recv().unwrap();
    installation_feedback_tx
        .send(crate::core::StatemapInstallationStatus::Success(res.0))
        .await
        .unwrap();
    assert_eq!(res.0, 13);
    // Send feedback for version 15
    let res = installation_rx.try_recv().unwrap();
    installation_feedback_tx
        .send(crate::core::StatemapInstallationStatus::Success(res.0))
        .await
        .unwrap();
    assert_eq!(res.0, 15);
    assert_eq!(installation_rx.max_capacity() - installation_rx.capacity(), 1);
}
