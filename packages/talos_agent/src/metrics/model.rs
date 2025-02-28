use crate::metrics::aggregates::{PercentileSet, Timeline};
use strum::Display;

/// The trackable event within Talos Agent
#[derive(Debug, Clone, PartialEq)]
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
    CandidateEnqueuedInAgent,
    CandidatePublishTaskStarting,
    CandidatePublishStarted,
    CandidatePublished,
    CandidateReceivedByTalos,
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
#[derive(Debug, Clone, Display, PartialEq)]
pub enum Signal {
    Start { time: u64, event: Event },
    End { time: u64, id: String, event_name: EventName },
}

/// Structure for aggregated report
#[derive(Debug)]
pub struct MetricsReport {
    pub times: Vec<Timeline>,
    pub outbox_1: PercentileSet,
    pub enqueing_2: PercentileSet,
    pub publish_taks_spawned_3: PercentileSet,
    pub candidate_publish_4: PercentileSet,
    pub candidate_kafka_trip_5: PercentileSet,
    pub decision_duration_6: PercentileSet,
    pub decision_download_7: PercentileSet,
    pub inbox_8: PercentileSet,
    pub total: PercentileSet,
    pub certify_min: Timeline,
    pub certify_max: Timeline,
    pub count: u64,
    pub publish_rate: f64,
}

impl MetricsReport {
    pub fn format_certify_max(&self) -> String {
        format!(
            "{:>8}: [{:>8}, {:>8}, {:>8}, {:>8}, {:>8}, {:>8}, {:>8}, {:>8}]",
            self.certify_max.total.as_micros(),
            self.certify_max.outbox_1.as_micros(),
            self.certify_max.enqueing_2.as_micros(),
            self.certify_max.publish_taks_spawn_3.as_micros(),
            self.certify_max.candidate_publish_4.as_micros(),
            self.certify_max.candidate_kafka_trip_5.as_micros(),
            self.certify_max.decision_duration_6.as_micros(),
            self.certify_max.decision_download_7.as_micros(),
            self.certify_max.inbox_8.as_micros(),
        )
    }

    pub fn format_certify_min(&self) -> String {
        format!(
            "{:>8}: [{:>8}, {:>8}, {:>8}, {:>8}, {:>8}, {:>8}, {:>8}, {:>8}]",
            self.certify_min.total.as_micros(),
            self.certify_min.outbox_1.as_micros(),
            self.certify_min.enqueing_2.as_micros(),
            self.certify_min.publish_taks_spawn_3.as_micros(),
            self.certify_min.candidate_publish_4.as_micros(),
            self.certify_min.candidate_kafka_trip_5.as_micros(),
            self.certify_min.decision_duration_6.as_micros(),
            self.certify_min.decision_download_7.as_micros(),
            self.certify_min.inbox_8.as_micros(),
        )
    }

    pub fn format_p99(&self) -> String {
        format!(
            "{:>8}: [{:>8}, {:>8}, {:>8}, {:>8}, {:>8}, {:>8}, {:>8}, {:>8}]",
            self.total.p99.value,
            self.outbox_1.p99.value,
            self.enqueing_2.p99.value,
            self.publish_taks_spawned_3.p99.value,
            self.candidate_publish_4.p99.value,
            self.candidate_kafka_trip_5.p99.value,
            self.decision_duration_6.p99.value,
            self.decision_download_7.p99.value,
            self.inbox_8.p99.value,
        )
    }

    pub fn format_p95(&self) -> String {
        format!(
            "{:>8}: [{:>8}, {:>8}, {:>8}, {:>8}, {:>8}, {:>8}, {:>8}, {:>8}]",
            self.total.p95.value,
            self.outbox_1.p95.value,
            self.enqueing_2.p95.value,
            self.publish_taks_spawned_3.p95.value,
            self.candidate_publish_4.p95.value,
            self.candidate_kafka_trip_5.p95.value,
            self.decision_duration_6.p95.value,
            self.decision_download_7.p95.value,
            self.inbox_8.p95.value,
        )
    }

    pub fn format_p90(&self) -> String {
        format!(
            "{:>8}: [{:>8}, {:>8}, {:>8}, {:>8}, {:>8}, {:>8}, {:>8}, {:>8}]",
            self.total.p90.value,
            self.outbox_1.p90.value,
            self.enqueing_2.p90.value,
            self.publish_taks_spawned_3.p90.value,
            self.candidate_publish_4.p90.value,
            self.candidate_kafka_trip_5.p90.value,
            self.decision_duration_6.p90.value,
            self.decision_download_7.p90.value,
            self.inbox_8.p90.value,
        )
    }

    pub fn format_p75(&self) -> String {
        format!(
            "{:>8}: [{:>8}, {:>8}, {:>8}, {:>8}, {:>8}, {:>8}, {:>8}, {:>8}]",
            self.total.p75.value,
            self.outbox_1.p75.value,
            self.enqueing_2.p75.value,
            self.publish_taks_spawned_3.p75.value,
            self.candidate_publish_4.p75.value,
            self.candidate_kafka_trip_5.p75.value,
            self.decision_duration_6.p75.value,
            self.decision_download_7.p75.value,
            self.inbox_8.p75.value,
        )
    }

    pub fn format_p50(&self) -> String {
        format!(
            "{:>8}: [{:>8}, {:>8}, {:>8}, {:>8}, {:>8}, {:>8}, {:>8}, {:>8}]",
            self.total.p50.value,
            self.outbox_1.p50.value,
            self.enqueing_2.p50.value,
            self.publish_taks_spawned_3.p50.value,
            self.candidate_publish_4.p50.value,
            self.candidate_kafka_trip_5.p50.value,
            self.decision_duration_6.p50.value,
            self.decision_download_7.p50.value,
            self.inbox_8.p50.value,
        )
    }

    pub fn get_rate(&self, duration_ms: u64) -> f64 {
        (self.count as f64) / (duration_ms as f64) * 1000.0
    }

    pub fn print(&self, duration_ms: u64, errors_count: u64) {
        let headers = r"'Rroundtrip (mcs)' ['client until agent 1 (mcs)', 'enqueued for in-flight tracing 2 (mcs)', 'publishing task spawn 3 (mcs)', 'kafka publishing 4 (mcs)', 'C. kafka trip 5 (mcs)','decision duration 6 (mcs)', 'D. kafka trip 7 (mcs)', 'agent to client 8 (mcs)']";

        tracing::warn!("");
        tracing::warn!("Publishing: {:>5.2} tps", self.publish_rate);
        tracing::warn!("Throuhput:  {:>5.2} tps", self.get_rate(duration_ms));
        tracing::warn!("");
        tracing::warn!("{}", headers);
        tracing::warn!("Max (mcs): {}", self.format_certify_max());
        tracing::warn!("Min (mcs): {}", self.format_certify_min());
        tracing::warn!("99% (mcs): {}", self.format_p99());
        tracing::warn!("95% (mcs): {}", self.format_p95());
        tracing::warn!("90% (mcs): {}", self.format_p90());
        tracing::warn!("75% (mcs): {}", self.format_p75());
        tracing::warn!("50% (mcs): {}", self.format_p50());
        tracing::warn!("");
        tracing::warn!("Min,Max,p75,p90,p95,Errors");
        tracing::warn!(
            "{},{},{},{},{},{}",
            self.certify_min.get_total_mcs(),
            self.certify_max.get_total_mcs(),
            self.total.p75.value,
            self.total.p90.value,
            self.total.p95.value,
            errors_count,
        )
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
            started_at: 0,
            outbox_1: Duration::from_secs(1),
            enqueing_2: Duration::from_secs(2),
            publish_taks_spawn_3: Duration::from_secs(3),
            candidate_publish_4: Duration::from_secs(4),
            candidate_kafka_trip_5: Duration::from_secs(5),
            decision_duration_6: Duration::from_secs(6),
            decision_download_7: Duration::from_secs(7),
            inbox_8: Duration::from_secs(8),
            total: Duration::from_secs(total_sec),
        }
    }

    fn sample_report() -> MetricsReport {
        let p_set = PercentileSet::new(&mut [], Timeline::get_total_mcs, |t| t.total.as_millis());
        MetricsReport {
            times: Vec::<Timeline>::new(),
            total: p_set.clone(),
            outbox_1: p_set.clone(),
            enqueing_2: p_set.clone(),
            publish_taks_spawned_3: p_set.clone(),
            candidate_publish_4: p_set.clone(),
            candidate_kafka_trip_5: p_set.clone(),
            decision_duration_6: p_set.clone(),
            decision_download_7: p_set.clone(),
            inbox_8: p_set,
            certify_max: tx("id1".to_string(), 1),
            certify_min: tx("id2".to_string(), 2),
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

        assert_eq!(
            report.format_p99(),
            "       0: [       0,        0,        0,        0,        0,        0,        0,        0]"
        );
        assert_eq!(
            report.format_p95(),
            "       0: [       0,        0,        0,        0,        0,        0,        0,        0]"
        );
        assert_eq!(
            report.format_p90(),
            "       0: [       0,        0,        0,        0,        0,        0,        0,        0]"
        );
        assert_eq!(
            report.format_p75(),
            "       0: [       0,        0,        0,        0,        0,        0,        0,        0]"
        );
        assert_eq!(
            report.format_p50(),
            "       0: [       0,        0,        0,        0,        0,        0,        0,        0]"
        );
        assert_eq!(
            report.format_certify_min(),
            " 2000000: [ 1000000,  2000000,  3000000,  4000000,  5000000,  6000000,  7000000,  8000000]"
        );
        assert_eq!(
            report.format_certify_max(),
            " 1000000: [ 1000000,  2000000,  3000000,  4000000,  5000000,  6000000,  7000000,  8000000]"
        );
    }

    #[test]
    fn report_should_compute_rate() {
        let report = sample_report();
        let _ = format!("debug and clone coverage {:?}", report);

        assert_eq!(report.get_rate(15_000), 0.6666666666666666);
    }
}
// $coverage:ignore-end
