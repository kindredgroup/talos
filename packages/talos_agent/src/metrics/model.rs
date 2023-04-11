use crate::metrics::aggregates::{PercentileSet, Timeline};
use strum::Display;

/// The trackable event within Talos Agent
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Event {
    pub id: String,
    pub event_name: EventName,
    pub time: u64,
}

/// Enum represents trackable events within Talos Agent
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

/// Data sent from metrics client to metrics service
#[derive(Debug, Clone, Display, Eq, PartialEq)]
pub enum Signal {
    Start { time: u64, event: Event },
    End { time: u64, id: String, event_name: EventName },
}

/// Structure for aggregated report
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

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::model::{Event, EventMetadata, EventName};
    use std::time::Duration;

    fn tx(id: String, total_sec: u64) -> Timeline {
        Timeline {
            id,
            started_at: 1,
            outbox: Duration::from_secs(1),
            publish: Duration::from_secs(2),
            candidate_publish_and_decision_time: Duration::from_secs(3),
            decision_download: Duration::from_secs(4),
            inbox: Duration::from_secs(5),
            total: Duration::from_secs(total_sec),
        }
    }

    fn sample_report() -> MetricsReport {
        let p_set = PercentileSet::new(&mut [], Timeline::get_total_ms, |t| t.total.as_millis());
        MetricsReport {
            times: Vec::<Timeline>::new(),
            outbox: p_set.clone(),
            candidate_publish_and_decision_time: p_set.clone(),
            decision_download: p_set.clone(),
            inbox: p_set.clone(),
            total: p_set.clone(),
            candidate_publish: p_set,
            certify_min: tx("id1".to_string(), 1),
            certify_max: tx("id2".to_string(), 2),
            count: 10,
            publish_rate: 0.0,
        }
    }

    #[test]
    fn clone_debug() {
        let event = Event {
            id: "id".to_string(),
            event_name: EventName::Started,
            time: 0,
        };

        let _ = format!("debug and clone coverage {:?}", event);

        let event_meta = EventMetadata {
            event: event.clone(),
            ended_at: None,
        };

        let event_meta_copy = event_meta;
        let _ = format!("debug and clone coverage {:?}", event_meta_copy);

        let signal_start = Signal::Start { event: event.clone(), time: 0 };
        let signal_end = Signal::End {
            event_name: event.event_name,
            time: 0,
            id: "id".to_string(),
        };

        let all = (signal_start, signal_end);
        let _ = format!("debug and clone coverage {:?}", all);
    }

    #[test]
    fn report_formatting() {
        let report = sample_report();
        let _ = format!("debug and clone coverage {:?}", report);

        assert_eq!(report.format_p99(), "  0.000: [  0.000,   0.000,   0.000,   0.000] (  0.000)");
        assert_eq!(report.format_p95(), "  0.000: [  0.000,   0.000,   0.000,   0.000] (  0.000)");
        assert_eq!(report.format_p90(), "  0.000: [  0.000,   0.000,   0.000,   0.000] (  0.000)");
        assert_eq!(report.format_p75(), "  0.000: [  0.000,   0.000,   0.000,   0.000] (  0.000)");
        assert_eq!(report.format_p50(), "  0.000: [  0.000,   0.000,   0.000,   0.000] (  0.000)");
        assert_eq!(report.format_certify_min(), "1000.000: [1000.000, 3000.000, 4000.000, 5000.000] (2000.000)");
        assert_eq!(report.format_certify_max(), "2000.000: [1000.000, 3000.000, 4000.000, 5000.000] (2000.000)");
    }

    #[test]
    fn report_should_compute_rate() {
        let report = sample_report();
        let _ = format!("debug and clone coverage {:?}", report);

        assert_eq!(report.get_rate(15_000), 0.6666666666666666);
    }
}
// $coverage:ignore-end
