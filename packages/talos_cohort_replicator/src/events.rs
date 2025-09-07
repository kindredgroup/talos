use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub type EventTimingsMap = HashMap<ReplicatorCandidateEvent, i128>;

#[derive(Debug, Default, Clone, PartialEq)]
pub struct StatemapEvents {
    timings: EventTimingsMap,
}

impl StatemapEvents {
    pub fn with_timings(event_timings: EventTimingsMap) -> Self {
        Self { timings: event_timings }
    }
}

impl ReplicatorCandidateEventTimingsTrait for StatemapEvents {
    fn record_event_timestamp(&mut self, event: ReplicatorCandidateEvent, ts_ns: i128) {
        self.timings.insert(event, ts_ns);
    }

    fn get_event_timestamp(&self, event: ReplicatorCandidateEvent) -> Option<i128> {
        self.timings.get(&event).copied()
    }

    fn get_all_timings(&self) -> EventTimingsMap {
        self.timings.clone()
    }
}

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
    /// When statemap installation is going to be called in the installer service.
    InstallerStatemapInstallionBegin,
    /// When statemap installation completed in the installer service.
    InstallerStatemapInstallionComplete,
}

pub trait ReplicatorCandidateEventTimingsTrait {
    /// Record the time in nanosecond precision for an event.
    fn record_event_timestamp(&mut self, event: ReplicatorCandidateEvent, ts_ns: i128);
    /// Get the time for an event.
    fn get_event_timestamp(&self, event: ReplicatorCandidateEvent) -> Option<i128>;
    /// Get all the events with their time
    fn get_all_timings(&self) -> EventTimingsMap;
}

// Below are some of the global events which could be exposed in future.
// Some help for diagnosis and some can be used for Otel metrics - Counters and Gauges.
// We already collect and publish metrics for some of these, but they are done from within each thread.
// We may want to expose it out to the consumer of the library to help build their own metrics or diagnosis of the library at a given time.
//
// - In Replicator Service
//  - Suffix size
//  - Suffix head
//  - Last candidate version received
//  - Last decision version received
//  - Total candidates received
//  - Total decision received
//  - Last commit version
//  - Last version whose statemap was sent for Queue service
//
// - In Queue Service
//  - Last version whose statemap was received
//  - Channel depth
//  - Queue size
//  - Queue head
//  - Last version whose statemap was sent to installer
//  - Last snapshot version while calling the update snapshot
//  - Last snapshot tracked internally
//
// - In Installer Service
//  - Last version whose statemap was received
//  - Channel depth
//  - No. of items currently being installed
//
// - Backpressure
//  - Backpressure max applied
//  - Backpressure total applied
//  - Number of times backpressure was applied
//
// - Errors
//  - Statemap Installation total Error count
//  - Snapshot update total Error count
