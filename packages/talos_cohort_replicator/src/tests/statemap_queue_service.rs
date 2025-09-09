use std::{
    collections::HashSet,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use axum::async_trait;
use serde_json::json;
use tokio::sync::mpsc::{self, error::TryRecvError};

use crate::{
    callbacks::ReplicatorSnapshotProvider,
    core::StatemapInstallState,
    events::StatemapEvents,
    services::statemap_queue_service::{self, StatemapQueueServiceConfig},
    StatemapItem, StatemapQueueChannelMessage,
};

#[derive(Default)]
pub struct MockSnapshotApi {
    pub snapshot: AtomicU64,
}

#[async_trait]
impl ReplicatorSnapshotProvider for MockSnapshotApi {
    async fn get_snapshot(&self) -> Result<u64, String> {
        let snapshot = self.snapshot.load(std::sync::atomic::Ordering::Relaxed);
        Ok(snapshot)
    }

    async fn update_snapshot(&self, version: u64) -> Result<(), String> {
        self.snapshot.store(version, std::sync::atomic::Ordering::Relaxed);
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
    let snapshot_api = MockSnapshotApi { snapshot: 8.into() };

    let mut queue_svc = statemap_queue_service::StatemapQueueService::new(
        statemaps_rx,
        installation_tx,
        statemap_installation_rx,
        replicator_feedback_tx,
        snapshot_api.into(),
        config,
        0,
        None,
    );

    tokio::spawn(async move {
        queue_svc.run().await.unwrap();
    });

    let statemap_item = StatemapItem::new("TestAction".to_string(), 10, json!({"version": 10}), Some(7));
    statemaps_tx
        .send(StatemapQueueChannelMessage::Message((10, vec![statemap_item], StatemapEvents::default())))
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
    let snapshot_api = MockSnapshotApi { snapshot: 8.into() };

    let mut queue_svc = statemap_queue_service::StatemapQueueService::new(
        statemaps_rx,
        installation_tx,
        statemap_installation_rx,
        replicator_feedback_tx,
        snapshot_api.into(),
        config,
        0,
        None,
    );

    tokio::spawn(async move {
        queue_svc.run().await.unwrap();
    });

    let statemap_item = StatemapItem::new("TestAction".to_string(), 7, json!({"version": 7}), Some(5));
    statemaps_tx
        .send(StatemapQueueChannelMessage::Message((7, vec![statemap_item], StatemapEvents::default())))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(10)).await;

    let receive_result = installation_rx.try_recv();
    assert_eq!(receive_result, Err(TryRecvError::Empty));
}

/// Test covers below possible scenarios that can impact the snapshot_version or items and their states in the queue
/// - Test Receiving and inserting message to queue
///      - when version received is less than the snapshot version.
///      - when version received is greater than the snapshot version.
/// - Test picking of statemaps for installation
///      - Check against `StatemapInstallState`
///      - Check safepoint against snapshot version
///      - Check safepoint to see which items are serialisable.
/// - Test feedback from install StatemapInstallationStatus::Success(version)
///      - When all version below are successfully installed.
///      - When there are some versions not installed yet.
/// - Test feedback from install StatemapInstallationStatus::Error
///      - Check when item is moved back to `Awaiting`, how does it behave if some other version has this version as safepoint
///      - Check when item is moved back to `Awaiting`, how does it behave if some other version doesn't have this version as safepoint
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
        snapshot: initial_snapshot_version.into(),
    };

    let mut queue_svc = statemap_queue_service::StatemapQueueService::new(
        statemaps_rx,
        installation_tx,
        installation_feedback_rx,
        replicator_feedback_tx,
        snapshot_api.into(),
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
        .send(StatemapQueueChannelMessage::Message((
            install_vers,
            vec![statemap_item],
            StatemapEvents::default(),
        )))
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
        .send(StatemapQueueChannelMessage::Message((
            install_vers,
            vec![statemap_item],
            StatemapEvents::default(),
        )))
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
        .send(StatemapQueueChannelMessage::Message((
            install_vers,
            vec![statemap_item],
            StatemapEvents::default(),
        )))
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
        .send(StatemapQueueChannelMessage::Message((
            install_vers,
            vec![statemap_item],
            StatemapEvents::default(),
        )))
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
        .send(StatemapQueueChannelMessage::Message((
            install_vers,
            vec![statemap_item],
            StatemapEvents::default(),
        )))
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
        .send(StatemapQueueChannelMessage::Message((
            install_vers,
            vec![statemap_item],
            StatemapEvents::default(),
        )))
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
        .send(StatemapQueueChannelMessage::Message((
            install_vers,
            vec![statemap_item],
            StatemapEvents::default(),
        )))
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
    let snapshot_api: Arc<MockSnapshotApi> = Arc::new(MockSnapshotApi {
        snapshot: initial_snapshot_version.into(),
    });

    let mut queue_svc = statemap_queue_service::StatemapQueueService::new(
        statemaps_rx,
        installation_tx,
        installation_feedback_rx,
        replicator_feedback_tx,
        snapshot_api.clone(),
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
        .send(StatemapQueueChannelMessage::Message((
            install_vers,
            vec![statemap_item],
            StatemapEvents::default(),
        )))
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
        .send(StatemapQueueChannelMessage::Message((
            install_vers,
            vec![statemap_item],
            StatemapEvents::default(),
        )))
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
        .send(StatemapQueueChannelMessage::Message((
            install_vers,
            vec![statemap_item],
            StatemapEvents::default(),
        )))
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
        .send(StatemapQueueChannelMessage::Message((
            install_vers,
            vec![statemap_item],
            StatemapEvents::default(),
        )))
        .await
        .unwrap();
    // Run the service to insert this into queue.
    queue_svc.run_once().await.unwrap();
    assert_eq!(installation_rx.max_capacity() - installation_rx.capacity(), 2);

    // We have inserted 4 items so length should be 4.
    assert_eq!(queue_svc.statemap_queue.queue.len(), 4);

    // Mock snapshot_api, update the snapshot to 19, simulating a scenario of some other instance must have updated the snapshot to persistance layer
    let _ = snapshot_api.update_snapshot(19).await;
    // Get next snapshot
    assert_eq!(snapshot_api.get_snapshot().await.unwrap(), 19);
    statemaps_tx.send(StatemapQueueChannelMessage::UpdateSnapshot).await.unwrap();

    // Run the service to update the snapshot + prune all items in queue below the snapshot_version.
    queue_svc.run_once().await.unwrap();

    // Only version 20 is above the snapshot, everything else can be removed.
    assert_eq!(queue_svc.statemap_queue.snapshot_version, 19);
    assert_eq!(queue_svc.statemap_queue.queue.len(), 1);

    // The two items send for install already + version 20 is send now because snapshot has moved to 19, which is above the safepoint of 18.
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

/// Test snapshot updates via callback.
/// - Test feedback of installation coming in out of order and how the snapshot is updated in the persisitance layer via callback.
#[tokio::test]
async fn test_snapshot_updated_via_callback() {
    let (statemaps_tx, statemaps_rx) = mpsc::channel(50);
    let (installation_feedback_tx, installation_feedback_rx) = mpsc::channel(50);
    let (installation_tx, mut installation_rx) = mpsc::channel(50);
    let (replicator_feedback_tx, _) = mpsc::channel(50);
    let config = StatemapQueueServiceConfig {
        queue_cleanup_frequency_ms: 30_000, // 30 mins
        enable_stats: false,
    };

    let initial_snapshot_version = 10;
    let snapshot_api: Arc<MockSnapshotApi> = Arc::new(MockSnapshotApi {
        snapshot: initial_snapshot_version.into(),
    });

    let mut queue_svc = statemap_queue_service::StatemapQueueService::new(
        statemaps_rx,
        installation_tx,
        installation_feedback_rx,
        replicator_feedback_tx,
        snapshot_api.clone(),
        config,
        0,
        None,
    );

    // Initial snapshot version is 0.
    assert_eq!(queue_svc.statemap_queue.snapshot_version, 0);
    // Send updatesnapshot to pick the `initial_snapshot_version`
    statemaps_tx.send(StatemapQueueChannelMessage::UpdateSnapshot).await.unwrap();

    // Run the service to update the snapshot + prune all items in queue below the snapshot_version.
    queue_svc.run_once().await.unwrap();
    assert_eq!(queue_svc.statemap_queue.snapshot_version, initial_snapshot_version);
    assert_eq!(queue_svc.get_last_saved_snapshot(), initial_snapshot_version);

    //*****/
    // install version is > snapshot_version and safepoint > snapshot_version, hence it will not be picked up for installation.
    //*****/
    let install_vers = 12;
    let safepoint = Some(10);
    let statemap_item = StatemapItem::new(
        "TestAction".to_string(),
        install_vers,
        json!({"mockPayload": format!("some mock data {install_vers}")}),
        safepoint,
    );
    statemaps_tx
        .send(StatemapQueueChannelMessage::Message((
            install_vers,
            vec![statemap_item],
            StatemapEvents::default(),
        )))
        .await
        .unwrap();
    // Run the service to insert this into queue.
    queue_svc.run_once().await.unwrap();

    //*****/
    // install version is > snapshot_version and safepoint > snapshot_version, hence it will not be picked up for installation.
    //*****/
    let install_vers = 13;
    let safepoint = Some(10);
    let statemap_item = StatemapItem::new(
        "TestAction".to_string(),
        install_vers,
        json!({"mockPayload": format!("some mock data {install_vers}")}),
        safepoint,
    );
    statemaps_tx
        .send(StatemapQueueChannelMessage::Message((
            install_vers,
            vec![statemap_item],
            StatemapEvents::default(),
        )))
        .await
        .unwrap();
    // Run the service to insert this into queue.
    queue_svc.run_once().await.unwrap();

    // There will be two items in the queue.
    assert_eq!(installation_rx.max_capacity() - installation_rx.capacity(), 2);

    let res_12 = installation_rx.try_recv().unwrap();
    let res_13 = installation_rx.try_recv().unwrap();
    // Send feedback for version 13
    installation_feedback_tx
        .send(crate::core::StatemapInstallationStatus::Success(res_13.0))
        .await
        .unwrap();

    // Receive the feedback for version 13
    queue_svc.run_once().await.unwrap();
    assert_eq!(queue_svc.get_last_saved_snapshot(), initial_snapshot_version);

    // Run the interval arm
    queue_svc.handle_interval_arm().await.unwrap();
    assert_eq!(queue_svc.get_last_saved_snapshot(), initial_snapshot_version);

    // Send feedback for version 12
    installation_feedback_tx
        .send(crate::core::StatemapInstallationStatus::Success(res_12.0))
        .await
        .unwrap();

    // Receive the feedback for version 12
    queue_svc.run_once().await.unwrap();
    assert_eq!(queue_svc.statemap_queue.snapshot_version, 13);

    // Run the interval arm
    queue_svc.handle_interval_arm().await.unwrap();

    // Sleep to give time for atomics to synchronize.
    tokio::time::sleep(Duration::from_millis(10)).await;
    // Assert the snapshot value has moved in the callback. Thus ensuring the persistance layer has updated the snapshot.
    assert_eq!(snapshot_api.get_snapshot().await.unwrap(), 13);
    assert_eq!(queue_svc.get_last_saved_snapshot(), 13);

    //
    // Receive version 19, 20, 23, 25
    // ** Version 19
    let install_vers = 19;
    let safepoint = Some(13);
    let statemap_item = StatemapItem::new(
        "TestAction".to_string(),
        install_vers,
        json!({"mockPayload": format!("some mock data {install_vers}")}),
        safepoint,
    );
    statemaps_tx
        .send(StatemapQueueChannelMessage::Message((
            install_vers,
            vec![statemap_item],
            StatemapEvents::default(),
        )))
        .await
        .unwrap();
    // Run the service to insert this into queue.
    queue_svc.run_once().await.unwrap();

    // ** Version 20
    let install_vers = 20;
    let safepoint = Some(13);
    let statemap_item = StatemapItem::new(
        "TestAction".to_string(),
        install_vers,
        json!({"mockPayload": format!("some mock data {install_vers}")}),
        safepoint,
    );
    statemaps_tx
        .send(StatemapQueueChannelMessage::Message((
            install_vers,
            vec![statemap_item],
            StatemapEvents::default(),
        )))
        .await
        .unwrap();
    // Run the service to insert this into queue.
    queue_svc.run_once().await.unwrap();

    // ** Version 23
    let install_vers = 23;
    let safepoint = Some(13);
    let statemap_item = StatemapItem::new(
        "TestAction".to_string(),
        install_vers,
        json!({"mockPayload": format!("some mock data {install_vers}")}),
        safepoint,
    );
    statemaps_tx
        .send(StatemapQueueChannelMessage::Message((
            install_vers,
            vec![statemap_item],
            StatemapEvents::default(),
        )))
        .await
        .unwrap();
    // Run the service to insert this into queue.
    queue_svc.run_once().await.unwrap();

    // ** Version 25
    let install_vers = 25;
    let safepoint = Some(13);
    let statemap_item = StatemapItem::new(
        "TestAction".to_string(),
        install_vers,
        json!({"mockPayload": format!("some mock data {install_vers}")}),
        safepoint,
    );
    statemaps_tx
        .send(StatemapQueueChannelMessage::Message((
            install_vers,
            vec![statemap_item],
            StatemapEvents::default(),
        )))
        .await
        .unwrap();
    // Run the service to insert this into queue.
    queue_svc.run_once().await.unwrap();
    queue_svc.run_once().await.unwrap();
    // Receive feedback for version 25, 20, 19, 23
    let res_19 = installation_rx.try_recv().unwrap();
    assert_eq!(res_19.0, 19);
    let res_20 = installation_rx.try_recv().unwrap();
    assert_eq!(res_20.0, 20);
    let res_23 = installation_rx.try_recv().unwrap();
    assert_eq!(res_23.0, 23);
    let res_25 = installation_rx.try_recv().unwrap();
    assert_eq!(res_25.0, 25);
    // Send feedback for version 25
    installation_feedback_tx
        .send(crate::core::StatemapInstallationStatus::Success(res_25.0))
        .await
        .unwrap();
    // Receive the feedback for version 25
    queue_svc.run_once().await.unwrap();
    // Send feedback for version 20
    installation_feedback_tx
        .send(crate::core::StatemapInstallationStatus::Success(res_20.0))
        .await
        .unwrap();
    // Receive the feedback for version 20
    queue_svc.run_once().await.unwrap();

    // Run the interval arm
    queue_svc.handle_interval_arm().await.unwrap();
    assert_eq!(snapshot_api.get_snapshot().await.unwrap(), 13);

    // After receiving feedback for 25 and 20. Snapshot remains the same.
    // Sleep to give time for atomics to synchronize.
    tokio::time::sleep(Duration::from_millis(10)).await;
    // Assert the snapshot value has not moved yet as we havent received feedback for 19.
    assert_eq!(snapshot_api.get_snapshot().await.unwrap(), 13);
    assert_eq!(queue_svc.get_last_saved_snapshot(), 13);
    // Receive feedback for 19, snapshot internal snapshot moves to 20
    // Send feedback for version 19
    installation_feedback_tx
        .send(crate::core::StatemapInstallationStatus::Success(res_19.0))
        .await
        .unwrap();
    // Receive the feedback for version 19
    queue_svc.run_once().await.unwrap();
    // Run interval arm, and internal and callback snapshot will synchornize.
    // Run the interval arm
    queue_svc.handle_interval_arm().await.unwrap();
    assert_eq!(snapshot_api.get_snapshot().await.unwrap(), 13);

    // After receiving feedback 19. Snapshot remains will move to 20.
    // Sleep to give time for atomics to synchronize.
    tokio::time::sleep(Duration::from_millis(10)).await;
    // Assert the snapshot value has not moved yet as we havent received feedback for 19.
    assert_eq!(snapshot_api.get_snapshot().await.unwrap(), 20);
    assert_eq!(queue_svc.get_last_saved_snapshot(), 20);
    //** Receive feedback 23, internal snapshot moves to 25

    // Send feedback for version 23
    installation_feedback_tx
        .send(crate::core::StatemapInstallationStatus::Success(res_23.0))
        .await
        .unwrap();
    // Receive the feedback for version 23
    queue_svc.run_once().await.unwrap();
    // Run interval arm, and internal and callback snapshot will synchornize.
    // Run the interval arm
    queue_svc.handle_interval_arm().await.unwrap();
    assert_eq!(snapshot_api.get_snapshot().await.unwrap(), 20);

    // Run interval arm, and internal and callback snapshot will synchornize.
    // After receiving feedback 23. Snapshot will move to 25.
    // Sleep to give time for atomics to synchronize.
    tokio::time::sleep(Duration::from_millis(10)).await;
    // Assert the snapshot value has not moved yet as we havent received feedback for 19.
    assert_eq!(snapshot_api.get_snapshot().await.unwrap(), 25);
    assert_eq!(queue_svc.get_last_saved_snapshot(), 25);
}

// Test effects of snapshot from persistance layer differing from the internal snapshot we track in memory.
#[tokio::test]
async fn test_queue_service_last_installed_snapshot_against_internal_snapshot() {
    let (statemaps_tx, statemaps_rx) = mpsc::channel(50);
    let (_installation_feedback_tx, installation_feedback_rx) = mpsc::channel(50);
    let (installation_tx, installation_rx) = mpsc::channel(50);
    let (replicator_feedback_tx, _) = mpsc::channel(50);
    let config = StatemapQueueServiceConfig {
        queue_cleanup_frequency_ms: 30_000, // 30 mins
        enable_stats: false,
    };

    let initial_snapshot_version = 10;
    let snapshot_api: Arc<MockSnapshotApi> = Arc::new(MockSnapshotApi {
        snapshot: initial_snapshot_version.into(),
    });

    let mut queue_svc = statemap_queue_service::StatemapQueueService::new(
        statemaps_rx,
        installation_tx,
        installation_feedback_rx,
        replicator_feedback_tx,
        snapshot_api.clone(),
        config,
        0,
        None,
    );

    // Initial snapshot version is 0.
    assert_eq!(queue_svc.statemap_queue.snapshot_version, 0);
    // Send updatesnapshot to pick the `initial_snapshot_version`
    statemaps_tx.send(StatemapQueueChannelMessage::UpdateSnapshot).await.unwrap();

    // Run the service to update the snapshot + prune all items in queue below the snapshot_version.
    queue_svc.run_once().await.unwrap();
    assert_eq!(queue_svc.statemap_queue.snapshot_version, initial_snapshot_version);
    assert_eq!(queue_svc.get_last_saved_snapshot(), initial_snapshot_version);

    assert_eq!(queue_svc.statemap_queue.queue.len(), 0);
    //***** Initial insert of versions < snapshot from persistance layer.
    // ** Version 8
    let install_vers = 8;
    let safepoint = Some(0);
    let statemap_item = StatemapItem::new(
        "TestAction".to_string(),
        install_vers,
        json!({"mockPayload": format!("some mock data {install_vers}")}),
        safepoint,
    );
    statemaps_tx
        .send(StatemapQueueChannelMessage::Message((
            install_vers,
            vec![statemap_item],
            StatemapEvents::default(),
        )))
        .await
        .unwrap();

    // handle `StatemapQueueChannelMessage` for version 8
    queue_svc.run_once().await.unwrap();

    assert_eq!(queue_svc.statemap_queue.queue.len(), 1);

    // As the version 8 is less than the snapshot version 10, it is already installed, and therefore if we receive again, this can be marked as installed, and not send for installation again.
    assert_eq!(
        queue_svc.statemap_queue.queue.get(&install_vers).unwrap().state,
        StatemapInstallState::Installed
    );

    //***** Initial insert of versions == snapshot from persistance layer.
    // ** Version 10
    let install_vers = 10;
    let safepoint = Some(0);
    let statemap_item = StatemapItem::new(
        "TestAction".to_string(),
        install_vers,
        json!({"mockPayload": format!("some mock data {install_vers}")}),
        safepoint,
    );
    statemaps_tx
        .send(StatemapQueueChannelMessage::Message((
            install_vers,
            vec![statemap_item],
            StatemapEvents::default(),
        )))
        .await
        .unwrap();

    // handle `StatemapQueueChannelMessage` for version 8
    queue_svc.run_once().await.unwrap();

    assert_eq!(queue_svc.statemap_queue.queue.len(), 2);

    // As the version 10 is equal to the snapshot version 10, it is already installed, and therefore if we receive again, this can be marked as installed, and not send for installation again.
    assert_eq!(
        queue_svc.statemap_queue.queue.get(&install_vers).unwrap().state,
        StatemapInstallState::Installed
    );
    // Nothing in the installation channel, as it is immediately marked as installed.
    assert_eq!(installation_rx.max_capacity() - installation_rx.capacity(), 0);

    //***** Insert of versions > snapshot
    // ** Version 12
    let install_vers = 12;
    let safepoint = Some(0);
    let statemap_item = StatemapItem::new(
        "TestAction".to_string(),
        install_vers,
        json!({"mockPayload": format!("some mock data {install_vers}")}),
        safepoint,
    );

    statemaps_tx
        .send(StatemapQueueChannelMessage::Message((
            install_vers,
            vec![statemap_item],
            StatemapEvents::default(),
        )))
        .await
        .unwrap();

    // handle `StatemapQueueChannelMessage` for version 12
    queue_svc.run_once().await.unwrap();

    // Item will be picked for installation and therefore
    assert_eq!(queue_svc.statemap_queue.queue.get(&install_vers).unwrap().state, StatemapInstallState::Inflight);
    // As the version > snapshot and there is no safepoint constraint, it will be send for installation.
    assert_eq!(installation_rx.max_capacity() - installation_rx.capacity(), 1);

    // Now we have two items in the queue, one is `installed` and the other is `inflight` for installer service to install.
    assert_eq!(queue_svc.statemap_queue.queue.len(), 3);

    // Run the interval arm, to synchronize.
    queue_svc.handle_interval_arm().await.unwrap();

    // Version 8 and 10 are pruned as it is already installed.
    assert_eq!(queue_svc.statemap_queue.queue.len(), 1);

    //***** Insert of versions > snapshot
    // ** Version 14
    let install_vers = 14;
    let safepoint = Some(0);
    let statemap_item = StatemapItem::new(
        "TestAction".to_string(),
        install_vers,
        json!({"mockPayload": format!("some mock data {install_vers}")}),
        safepoint,
    );

    statemaps_tx
        .send(StatemapQueueChannelMessage::Message((
            install_vers,
            vec![statemap_item],
            StatemapEvents::default(),
        )))
        .await
        .unwrap();

    // handle `StatemapQueueChannelMessage` for version 14
    queue_svc.run_once().await.unwrap();

    // Version 12 and 14 are `inflight` hence, 2 items in queue.
    assert_eq!(queue_svc.statemap_queue.queue.len(), 2);

    // Scenario where persistance layer was updated to snapshot 13
    let _ = snapshot_api.update_snapshot(13).await;
    // Assert next snapshot is 13.
    assert_eq!(snapshot_api.get_snapshot().await.unwrap(), 13);
    statemaps_tx.send(StatemapQueueChannelMessage::UpdateSnapshot).await.unwrap();

    // Process the `UpdateSnapshot` message
    queue_svc.run_once().await.unwrap();

    // Irrespective of the installation feedback for version 12, we will prune it.
    assert_eq!(queue_svc.statemap_queue.queue.len(), 1);

    // Scenario where persistance layer was updated to snapshot 14
    let _ = snapshot_api.update_snapshot(14).await;
    // Assert next snapshot is 14.
    assert_eq!(snapshot_api.get_snapshot().await.unwrap(), 14);
    statemaps_tx.send(StatemapQueueChannelMessage::UpdateSnapshot).await.unwrap();

    // Process the `UpdateSnapshot` message
    queue_svc.run_once().await.unwrap();

    // Irrespective of the installation feedback for version 14, we will prune it. Therefore there are no items in the queue.
    assert_eq!(queue_svc.statemap_queue.queue.len(), 0);
    assert_eq!(queue_svc.statemap_queue.snapshot_version, 14)
}
