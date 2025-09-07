use std::sync::Arc;

use axum::async_trait;
use time::{Duration, OffsetDateTime};
use tokio::sync::mpsc;

use crate::{
    callbacks::ReplicatorInstaller,
    events::{ReplicatorCandidateEventTimingsTrait, StatemapEvents},
    services::statemap_installer_service::statemap_install_future,
    StatemapItem,
};

struct MockReplicatorInstaller;

#[async_trait]
impl ReplicatorInstaller for MockReplicatorInstaller {
    async fn install(&self, _sm: Vec<StatemapItem>, _version: u64) -> Result<(), String> {
        Ok(())
    }
}

// Test the install event is set in StatemapEvents
#[tokio::test]
async fn test_install_event_set() {
    let (statemap_installation_tx, _statemap_installation_rx) = mpsc::channel(50);
    let mut statemap_events = StatemapEvents::default();
    statemap_install_future(Arc::new(MockReplicatorInstaller), statemap_installation_tx, vec![], &mut statemap_events, 10).await;
    assert_eq!(statemap_events.get_all_timings().len(), 2); // Install begin and complete events added
    assert!(statemap_events
        .get_event_timestamp(crate::events::ReplicatorCandidateEvent::InstallerStatemapInstallionBegin)
        .is_some())
}
// Test the events passed into the future is retained + install event is set in StatemapEvents
#[tokio::test]
async fn test_propogated_events_retained() {
    let (statemap_installation_tx, _statemap_installation_rx) = mpsc::channel(50);
    let mut statemap_events = StatemapEvents::default();
    statemap_events.record_event_timestamp(
        crate::events::ReplicatorCandidateEvent::QueueStatemapReceived,
        (OffsetDateTime::now_utc() - Duration::milliseconds(20)).unix_timestamp_nanos(),
    );
    statemap_events.record_event_timestamp(
        crate::events::ReplicatorCandidateEvent::QueueStatemapSent,
        (OffsetDateTime::now_utc() - Duration::milliseconds(20)).unix_timestamp_nanos(),
    );
    statemap_events.record_event_timestamp(
        crate::events::ReplicatorCandidateEvent::InstallerStatemapReceived,
        (OffsetDateTime::now_utc() - Duration::milliseconds(10)).unix_timestamp_nanos(),
    );
    statemap_install_future(Arc::new(MockReplicatorInstaller), statemap_installation_tx, vec![], &mut statemap_events, 10).await;
    assert_eq!(statemap_events.get_all_timings().len(), 5); // The three events sent + install events added
    assert!(statemap_events
        .get_event_timestamp(crate::events::ReplicatorCandidateEvent::InstallerStatemapInstallionComplete)
        .is_some())
}
