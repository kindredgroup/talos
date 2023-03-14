use crate::metrics::aggregates::{PercentileSet, Timeline};
use strum::Display;

#[derive(Debug, Clone)]
pub struct Event {
    pub id: String,
    pub event_name: EventName,
    pub time: u64,
}

#[derive(Debug, Clone, Display, PartialEq, Eq, Hash)]
pub enum EventName {
    // Transaction started
    Started,
    // Span represents a time from the moment client formulated certification request to the moment it has been received by agent
    CandidateReceived,
    CandidatePublished,
    // Time when decision is decided by Talos (the remote Talos time)
    Decided,
    // Decision is received by agent, but hasn't been passed back to the client
    DecisionReceived,
    // Transaction finished
    Finished,
}

#[derive(Clone, Debug)]
pub struct EventMetadata {
    pub event: Event,
    pub ended_at: Option<u64>,
}

#[derive(Debug, Clone, Display)]
pub enum Signal {
    Start { time: u64, event: Event },
    End { time: u64, id: String, event_name: EventName },
}

#[derive(Debug)]
pub struct MetricsReport {
    pub times: Vec<Timeline>,
    pub outbox: PercentileSet,
    pub candidate_publish_and_decision_time: PercentileSet,
    pub decision_download: PercentileSet,
    pub inbox: PercentileSet,
    pub total: PercentileSet,
    pub candidate_publish: PercentileSet,
    pub certify_min: Timeline,
    pub certify_max: Timeline,
    pub count: u64,
    pub publish_rate: f64,
}

impl MetricsReport {
    pub fn format_certify_max(&self) -> String {
        format!(
            "{:>7.3}: [{:>7.3}, {:>7.3}, {:>7.3}, {:>7.3}] ({:>7.3})",
            self.certify_max.total.as_micros() as f64 / 1000_f64,
            self.certify_max.outbox.as_micros() as f64 / 1000_f64,
            self.certify_max.candidate_publish_and_decision_time.as_micros() as f64 / 1000_f64,
            self.certify_max.decision_download.as_micros() as f64 / 1000_f64,
            self.certify_max.inbox.as_micros() as f64 / 1000_f64,
            self.certify_max.publish.as_micros() as f64 / 1000_f64,
        )
    }

    pub fn format_certify_min(&self) -> String {
        format!(
            "{:>7.3}: [{:>7.3}, {:>7.3}, {:>7.3}, {:>7.3}] ({:>7.3})",
            self.certify_min.total.as_micros() as f64 / 1000_f64,
            self.certify_min.outbox.as_micros() as f64 / 1000_f64,
            self.certify_min.candidate_publish_and_decision_time.as_micros() as f64 / 1000_f64,
            self.certify_min.decision_download.as_micros() as f64 / 1000_f64,
            self.certify_min.inbox.as_micros() as f64 / 1000_f64,
            self.certify_min.publish.as_micros() as f64 / 1000_f64,
        )
    }

    pub fn format_p99(&self) -> String {
        format!(
            "{:>7.3}: [{:>7.3}, {:>7.3}, {:>7.3}, {:>7.3}] ({:>7.3})",
            self.total.p99.value,
            self.outbox.p99.value,
            self.candidate_publish_and_decision_time.p99.value,
            self.decision_download.p99.value,
            self.inbox.p99.value,
            self.candidate_publish.p99.value,
        )
    }

    pub fn format_p95(&self) -> String {
        format!(
            "{:>7.3}: [{:>7.3}, {:>7.3}, {:>7.3}, {:>7.3}] ({:>7.3})",
            self.total.p95.value,
            self.outbox.p95.value,
            self.candidate_publish_and_decision_time.p95.value,
            self.decision_download.p95.value,
            self.inbox.p95.value,
            self.candidate_publish.p95.value,
        )
    }

    pub fn format_p90(&self) -> String {
        format!(
            "{:>7.3}: [{:>7.3}, {:>7.3}, {:>7.3}, {:>7.3}] ({:>7.3})",
            self.total.p90.value,
            self.outbox.p90.value,
            self.candidate_publish_and_decision_time.p90.value,
            self.decision_download.p90.value,
            self.inbox.p90.value,
            self.candidate_publish.p90.value,
        )
    }

    pub fn format_p75(&self) -> String {
        format!(
            "{:>7.3}: [{:>7.3}, {:>7.3}, {:>7.3}, {:>7.3}] ({:>7.3})",
            self.total.p75.value,
            self.outbox.p75.value,
            self.candidate_publish_and_decision_time.p75.value,
            self.decision_download.p75.value,
            self.inbox.p75.value,
            self.candidate_publish.p75.value,
        )
    }

    pub fn format_p50(&self) -> String {
        format!(
            "{:>7.3}: [{:>7.3}, {:>7.3}, {:>7.3}, {:>7.3}] ({:>7.3})",
            self.total.p50.value,
            self.outbox.p50.value,
            self.candidate_publish_and_decision_time.p50.value,
            self.decision_download.p50.value,
            self.inbox.p50.value,
            self.candidate_publish.p50.value,
        )
    }

    pub fn get_rate(&self, duration_ms: u64) -> f64 {
        (self.count as f64) / (duration_ms as f64) * 1000.0
    }
}
