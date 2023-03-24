use crate::metrics::aggregates::{PercentileSet, Timeline};
use crate::metrics::model::{EventMetadata, EventName, MetricsReport, Signal};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The internal Metrics service responsible for collecting and accumulating runtime events
pub struct Metrics {
    state: Arc<Mutex<HashMap<String, HashMap<EventName, EventMetadata>>>>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        Metrics {
            state: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Locates event in the given list and extracts its time or falls back to default value.
    fn get_time(events: &HashMap<EventName, EventMetadata>, event: &EventName) -> u64 {
        events.get(event).map(|data| data.event.time).unwrap_or(u64::MAX)
    }

    /// Analyses collected data and produces metrics report.
    pub fn collect(&self) -> Option<MetricsReport> {
        let data = self.state.lock().unwrap();

        let mut start_max = 0_u64;
        let mut start_min = u64::MAX;
        let mut certify_min = u64::MAX;
        let mut certify_min_time: Option<Timeline> = None;
        let mut certify_max = 0_u64;
        let mut certify_max_time: Option<Timeline> = None;

        let mut times: Vec<Timeline> = Vec::new();
        for (id, events) in data.iter() {
            let started_at = Self::get_time(events, &EventName::Started);
            if started_at == u64::MAX {
                // Skip incomplete transaction
                continue;
            }

            let finished_at = events.get(&EventName::Finished).map(|data| data.event.time).unwrap_or(u64::MAX);
            let candidate_received_at = events.get(&EventName::CandidateReceived).map(|data| data.event.time).unwrap_or(u64::MAX);
            let candidate_published_at = events.get(&EventName::CandidatePublished).map(|data| data.event.time).unwrap_or(u64::MAX);
            let decided_at = events.get(&EventName::Decided).map(|data| data.event.time).unwrap_or(u64::MAX);
            let decision_received_at = events.get(&EventName::DecisionReceived).map(|data| data.event.time).unwrap_or(u64::MAX);

            let total = finished_at - started_at;

            let time = Timeline {
                id: id.to_string(),
                started_at,
                outbox: Duration::from_nanos(candidate_received_at - started_at),
                publish: Duration::from_nanos(candidate_published_at - candidate_received_at),
                candidate_publish_and_decision_time: Duration::from_nanos(decided_at - candidate_received_at),
                decision_download: Duration::from_nanos(decision_received_at - decided_at),
                inbox: Duration::from_nanos(finished_at - decision_received_at),
                total: Duration::from_nanos(total),
            };

            if started_at < start_min {
                start_min = started_at
            }
            if started_at > start_max {
                start_max = started_at
            }
            if total < certify_min {
                certify_min = total;
                certify_min_time = Some(time.clone());
            }
            if total > certify_max {
                certify_max = total;
                certify_max_time = Some(time.clone());
            }

            times.push(time);
        }

        if start_min == u64::MAX {
            return None;
        }

        let count = data.len() as u64;
        let publish_rate = count as f64 / Duration::from_nanos(start_max - start_min).as_secs_f64();

        Some(MetricsReport {
            times: times.clone(),
            total: PercentileSet::new(&mut times, Timeline::get_total_ms, |i| i.total.as_micros()),
            outbox: PercentileSet::new(&mut times, Timeline::get_outbox_ms, |i| i.outbox.as_micros()),
            candidate_publish_and_decision_time: PercentileSet::new(&mut times, Timeline::get_candidate_publish_and_decision_time_ms, |i| {
                i.candidate_publish_and_decision_time.as_micros()
            }),
            decision_download: PercentileSet::new(&mut times, Timeline::get_decision_download_ms, |i| i.decision_download.as_micros()),
            inbox: PercentileSet::new(&mut times, Timeline::get_inbox_ms, |i| i.inbox.as_micros()),
            candidate_publish: PercentileSet::new(&mut times, Timeline::get_publish_ms, |i| i.publish.as_micros()),
            certify_max: certify_max_time.unwrap(),
            certify_min: certify_min_time.unwrap(),
            count,
            publish_rate,
        })
    }

    /// Launches background task which collects and stores incoming signals
    pub fn run<TSignalRx: crate::mpsc::core::Receiver<Data = Signal> + 'static>(&self, mut rx_destination: TSignalRx) {
        let state = Arc::clone(&self.state);
        tokio::spawn(async move {
            loop {
                let result = rx_destination.recv().await;
                if result.is_none() {
                    continue;
                }

                let action = result.unwrap();
                match action {
                    Signal::Start { time: _, event } => {
                        let key = event.id.clone();
                        let data = EventMetadata {
                            event: event.clone(),
                            ended_at: None,
                        };

                        let mut map = state.lock().unwrap();
                        if map.contains_key(key.clone().as_str()) {
                            let events = map.get_mut(key.as_str()).unwrap();
                            events.insert(event.event_name, data);
                        } else {
                            map.insert(key.clone(), HashMap::from([(event.event_name, data)]));
                        }
                    }
                    Signal::End { id, time, event_name } => {
                        if let Some(events) = state.lock().unwrap().get_mut(&id) {
                            if let Some(data) = events.get_mut(&event_name) {
                                data.ended_at = Some(time);
                            }
                        }
                    }
                }
            }
        });
    }
}

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::model::Event;

    fn new_event(id: &str, name: EventName, time: u64) -> EventMetadata {
        EventMetadata {
            event: Event {
                id: id.to_string(),
                event_name: name,
                time,
            },
            ended_at: None,
        }
    }

    fn insert_event(state: &mut HashMap<String, HashMap<EventName, EventMetadata>>, id: &str, name: EventName, time: u64) {
        let event = new_event(id, name.clone(), time);
        if state.contains_key(id) {
            let _ = state.get_mut(id).unwrap().insert(name, event);
        } else {
            let _ = state.insert(id.to_string(), HashMap::from([(name, event)]));
        }
    }

    fn nanos(sec: u64) -> u64 {
        sec * 1_000_000_000
    }

    fn millis(sec: u64) -> u128 {
        sec as u128 * 1_000_u128
    }

    #[test]
    fn new_should_create_empty_state() {
        // this is really for coverage
        let m = Metrics::default();
        assert!(m.state.lock().unwrap().is_empty());
    }

    #[test]
    fn get_time_should_extract_event_time() {
        // this is really for coverage
        let mut events = HashMap::new();
        events.insert(EventName::Decided, new_event("1", EventName::Decided, 1111));
        assert_eq!(Metrics::get_time(&events, &EventName::Decided), 1111);
    }

    #[test]
    fn should_collect_none_without_start_event() {
        let mut server = Metrics::default();
        let mut state = HashMap::new();
        insert_event(&mut state, "1", EventName::Decided, u64::MAX);

        server.state = Arc::new(Mutex::new(state));
        let report = server.collect();
        assert!(report.is_none());
    }

    #[test]
    fn collect() {
        let mut server = Metrics::default();
        let mut state = HashMap::new();
        insert_event(&mut state, "1", EventName::Started, nanos(1));
        insert_event(&mut state, "1", EventName::CandidateReceived, nanos(2));
        insert_event(&mut state, "1", EventName::CandidatePublished, nanos(3));
        insert_event(&mut state, "1", EventName::Decided, nanos(4));
        insert_event(&mut state, "1", EventName::DecisionReceived, nanos(5));
        insert_event(&mut state, "1", EventName::Finished, nanos(6));

        insert_event(&mut state, "2", EventName::Started, nanos(2));
        insert_event(&mut state, "2", EventName::CandidateReceived, nanos(3));
        insert_event(&mut state, "2", EventName::CandidatePublished, nanos(4));
        insert_event(&mut state, "2", EventName::Decided, nanos(5));
        insert_event(&mut state, "2", EventName::DecisionReceived, nanos(6));
        insert_event(&mut state, "2", EventName::Finished, nanos(8));

        server.state = Arc::new(Mutex::new(state));
        let maybe_report = server.collect();
        assert!(maybe_report.is_some());

        let report = maybe_report.unwrap();
        assert_eq!(report.count, 2_u64);
        assert_eq!(report.times.len(), 2);
        assert_eq!(report.total.p99.value, 6_000_f32);
        assert_eq!(report.total.p95.value, 6_000_f32);
        assert_eq!(report.total.p90.value, 6_000_f32);
        assert_eq!(report.total.p75.value, 6_000_f32);
        assert_eq!(report.total.p50.value, 5_000_f32);
        assert_eq!(report.publish_rate, 2_f64);
        assert_eq!(report.certify_min.total.as_millis(), millis(5));
        assert_eq!(report.certify_max.total.as_millis(), millis(6));
    }
}
// $coverage:ignore-end
