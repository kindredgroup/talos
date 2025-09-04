use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub type EventTimingsMap = HashMap<ReplicatorEvents, i128>;

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, Hash)]
pub enum ReplicatorEvents {
    // Replicator Service Thread events
    CandidateReceived,
    DecisionReceived,
    StatemapPicked,
    // Statemap Queue Service Thread events
    StatemapReceivedAtQueueService,
    StatemapSendToInstallService,
    // Install Service Thread events
    StatmapReceivedAtInstallService,
    StatemapInstalled,
}

pub trait EventTimingsTrait {
    fn set_timing_for_event(&mut self, event: ReplicatorEvents, ts: i128);
    fn get_timing_by_event(&self, event: ReplicatorEvents) -> Option<i128>;
    fn get_event_timings(&self) -> EventTimingsMap;
}
