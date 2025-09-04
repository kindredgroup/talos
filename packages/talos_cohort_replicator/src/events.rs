use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub type EventTimingsMap = HashMap<ReplicatorCandidateEvent, i128>;

/// Capture the various events from the time a candidate arrived in replicator to when it's statemaps where installed.
/// The various events can be used to capture the time taken in the journey between various events.
///
/// eg: Can be used to collect metrics at various key journey events.
#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, Hash)]
pub enum ReplicatorCandidateEvent {
    // ** Events captured for candidate on the Replicator Service Thread.
    /// When the candidate message was consumed in the replicator
    ReplicatorCandidateReceived,
    /// When the decision message was consumed in the replicator
    ReplicatorDecisionReceived,
    /// When the statemap is picked and ready to send to the queue service
    ReplicatorStatemapPicked,

    // ** Events captured for statemaps associated with the candidate on the Statemap Queue Service thread.
    /// When the statemap is received in the queue service
    QueueStatemapReceived,
    /// When the statemap is sent from the queue service to installer service
    QueueStatemapSent,

    // ** Events captured for statemaps associated with the candidate on the Statemap Installer thread.
    /// When statemap is received by installer.
    InstallerStatemapReceived,
    /// When statemap installation completed in the installer service.
    InstallerStatemapInstalled,
}

pub trait EventTimingsTrait {
    /// Record the time in nanosecond precision for an event.
    fn record_event(&mut self, event: ReplicatorCandidateEvent, ts_ns: i128);
    /// Get the time for an event.
    fn get_event_timestamp(&self, event: ReplicatorCandidateEvent) -> Option<i128>;
    /// Get all the events with their time
    fn get_all_timings(&self) -> EventTimingsMap;
}
