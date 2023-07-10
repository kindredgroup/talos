use tokio::sync::Mutex;

use crate::metrics::aggregates::{PercentileSet, Timeline};
use crate::metrics::model::{EventMetadata, EventName, MetricsReport, Signal};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// The internal Metrics service responsible for collecting and accumulating runtime events
#[derive(Debug)]
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
    pub async fn collect(&self) -> Option<MetricsReport> {
        let data = self.state.lock().await;

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
            let finished_at = Self::get_time(events, &EventName::Finished);
            let candidate_received_at = Self::get_time(events, &EventName::CandidateReceived);
            let candidate_enqueued_at = Self::get_time(events, &EventName::CandidateEnqueuedInAgent);
            let candidate_publish_task_starting_at = Self::get_time(events, &EventName::CandidatePublishTaskStarting);
            let candidate_publish_started_at = Self::get_time(events, &EventName::CandidatePublishStarted);
            let mut candidate_published_at = Self::get_time(events, &EventName::CandidatePublished);
            let recieved_by_talos_at = Self::get_time(events, &EventName::CandidateReceivedByTalos);

            if recieved_by_talos_at < candidate_published_at {
                // if talos is faster receiving than publisher call unblocks
                candidate_published_at = recieved_by_talos_at
            }

            let decided_at = Self::get_time(events, &EventName::Decided);
            let decision_received_at = Self::get_time(events, &EventName::DecisionReceived);

            // Skip incomplete transaction
            if finished_at == u64::MAX
                || candidate_received_at == u64::MAX
                || candidate_enqueued_at == u64::MAX
                || candidate_publish_task_starting_at == u64::MAX
                || candidate_publish_started_at == u64::MAX
                || candidate_published_at == u64::MAX
                || recieved_by_talos_at == u64::MAX
                || decided_at == u64::MAX
                || decision_received_at == u64::MAX
            {
                continue;
            }

            let total = finished_at - started_at;

            let time = Timeline {
                id: id.to_string(),
                started_at,
                outbox_1: Duration::from_nanos(candidate_received_at - started_at),
                enqueing_2: Duration::from_nanos(candidate_enqueued_at - candidate_received_at),
                publish_taks_spawn_3: Duration::from_nanos(candidate_publish_started_at - candidate_publish_task_starting_at),
                candidate_publish_4: Duration::from_nanos(candidate_published_at - candidate_publish_started_at),
                candidate_kafka_trip_5: Duration::from_nanos(recieved_by_talos_at - candidate_published_at),
                decision_duration_6: Duration::from_nanos(decided_at - recieved_by_talos_at),
                decision_download_7: Duration::from_nanos(decision_received_at - decided_at),
                inbox_8: Duration::from_nanos(finished_at - decision_received_at),
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

        let sorted = &mut times.to_owned();
        let mut sum = Timeline {
            id: "times".into(),
            started_at: 0,
            outbox_1: Duration::from_nanos(0),
            enqueing_2: Duration::from_nanos(0),
            publish_taks_spawn_3: Duration::from_nanos(0),
            candidate_publish_4: Duration::from_nanos(0),
            candidate_kafka_trip_5: Duration::from_nanos(0),
            decision_duration_6: Duration::from_nanos(0),
            decision_download_7: Duration::from_nanos(0),
            inbox_8: Duration::from_nanos(0),
            total: Duration::from_nanos(0),
        };
        sorted.sort_by_key(Timeline::get_started_at);

        let mut earliest_time = 0_u64;
        let mut total_count = 0_u64;
        let mut count_in_bucket = 0_u64;
        let mut bucket = 1_f32;
        let bucket_size = 1_000_000_000_f32;
        let mut bucket_limit = 1_000_000_000_f32;
        let mut elapsed = 0_u64;
        let progress_frequency = if sorted.len() > 10 { (sorted.len() / 10) as u64 } else { 1 };

        log::warn!(
            "METRIC agent-spans(header): {},{},{},{},{},{},{},{},{},{},{},'-',{},{}",
            "total count",
            "roundtrip (mcs)",
            "client until agent 1 (mcs)",
            "enqueued for in-flight tracing 2 (mcs)",
            "publishing task spawn 3 (mcs)",
            "kafka publishing 4 (mcs)",
            "C. kafka trip 5 (mcs)",
            "decision duration 6 (mcs) ",
            "D. kafka trip 7 (mcs)",
            "agent to client 8 (mcs)",
            "since first sample (mcs)",
            "bucket number",
            "samples in bucket",
        );

        for tx in sorted.iter() {
            total_count += 1;
            count_in_bucket += 1;

            //log::error!("debug: {}, {}, {}, {}", tx.id, tx.candidate_publish_4.as_micros(), tx.candidate_kafka_trip_5.as_micros(), tx.decision_duration_6.as_micros());

            if total_count % progress_frequency == 0 {
                log::warn!("METRIC agent-spans(progress): {} of {}", total_count, sorted.len());
            }
            if earliest_time == 0 {
                earliest_time = tx.started_at;
            }

            elapsed = tx.started_at - earliest_time;
            if elapsed > bucket_limit as u64 {
                log::warn!(
                    "METRIC agent-spans: {},{},{},{},{},{},{},{},{},{},{},'-',{},{}",
                    total_count,
                    sum.total.as_micros(),
                    sum.outbox_1.as_micros(),
                    sum.enqueing_2.as_micros(),
                    sum.publish_taks_spawn_3.as_micros(),
                    sum.candidate_publish_4.as_micros(),
                    sum.candidate_kafka_trip_5.as_micros(),
                    sum.decision_duration_6.as_micros(),
                    sum.decision_download_7.as_micros(),
                    sum.inbox_8.as_micros(),
                    Duration::from_nanos(elapsed).as_micros(),
                    bucket,
                    count_in_bucket,
                );
                count_in_bucket = 0;
                bucket += 1.0;
                bucket_limit = bucket * bucket_size;
            } else {
                // let d = sum.candidate_kafka_trip_5 + tx.candidate_kafka_trip_5;
                // if d.as_micros() * 1000 > 18446744073709551615 {
                //     log::error!("{} sum.candidate_kafka_trip_5: sum: {} mcs, tx: {} mcs id: {}", bucket, d.as_micros(), tx.candidate_kafka_trip_5.as_micros(), tx.id);
                // }
                sum.outbox_1 += tx.outbox_1;
                sum.enqueing_2 += tx.enqueing_2;
                sum.publish_taks_spawn_3 += tx.publish_taks_spawn_3;
                sum.candidate_publish_4 += tx.candidate_publish_4;
                sum.candidate_kafka_trip_5 += tx.candidate_kafka_trip_5;
                sum.decision_duration_6 += tx.decision_duration_6;
                sum.decision_download_7 += tx.decision_download_7;
                sum.inbox_8 += tx.inbox_8;
                sum.total += tx.total;
            }
        }

        log::warn!(
            "METRIC agent-spans: {},{},{},{},{},{},{},{},{},{},{},'-',{},{}",
            total_count,
            sum.total.as_micros(),
            sum.outbox_1.as_micros(),
            sum.enqueing_2.as_micros(),
            sum.publish_taks_spawn_3.as_micros(),
            sum.candidate_publish_4.as_micros(),
            sum.candidate_kafka_trip_5.as_micros(),
            sum.decision_duration_6.as_micros(),
            sum.decision_download_7.as_micros(),
            sum.inbox_8.as_micros(),
            Duration::from_nanos(elapsed).as_micros(),
            bucket,
            count_in_bucket,
        );

        if start_min == u64::MAX {
            return None;
        }

        let count = data.len() as u64;
        let publish_rate = count as f64 / Duration::from_nanos(start_max - start_min).as_secs_f64();

        Some(MetricsReport {
            times: times.clone(),
            total: PercentileSet::new(&mut times, Timeline::get_total_mcs, |i| i.total.as_micros()),
            outbox_1: PercentileSet::new(&mut times, Timeline::get_outbox_mcs, |i| i.outbox_1.as_micros()),
            enqueing_2: PercentileSet::new(&mut times, Timeline::get_enqueing_mcs, |i| i.enqueing_2.as_micros()),
            publish_taks_spawned_3: PercentileSet::new(&mut times, Timeline::get_publish_taks_spawn_mcs, |i| i.publish_taks_spawn_3.as_micros()),
            candidate_publish_4: PercentileSet::new(&mut times, Timeline::get_candidate_publish_mcs, |i| i.candidate_publish_4.as_micros()),
            candidate_kafka_trip_5: PercentileSet::new(&mut times, Timeline::get_candidate_kafka_trip_mcs, |i| i.candidate_kafka_trip_5.as_micros()),
            decision_duration_6: PercentileSet::new(&mut times, Timeline::get_decision_duration_mcs, |i| i.decision_duration_6.as_micros()),
            decision_download_7: PercentileSet::new(&mut times, Timeline::get_decision_download_mcs, |i| i.decision_download_7.as_micros()),
            inbox_8: PercentileSet::new(&mut times, Timeline::get_inbox_mcs, |i| i.inbox_8.as_micros()),
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

                        let mut map = state.lock().await;
                        if map.contains_key(key.clone().as_str()) {
                            let events = map.get_mut(key.as_str()).unwrap();
                            events.insert(event.event_name, data);
                        } else {
                            map.insert(key.clone(), HashMap::from([(event.event_name, data)]));
                        }
                    }
                    Signal::End { id, time, event_name } => {
                        if let Some(events) = state.lock().await.get_mut(&id) {
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

    #[tokio::test]
    async fn new_should_create_empty_state() {
        // this is really for coverage
        let m = Metrics::default();
        assert!(m.state.lock().await.is_empty());
    }

    #[test]
    fn get_time_should_extract_event_time() {
        // this is really for coverage
        let mut events = HashMap::new();
        events.insert(EventName::Decided, new_event("1", EventName::Decided, 1111));
        assert_eq!(Metrics::get_time(&events, &EventName::Decided), 1111);
    }

    #[tokio::test]
    async fn should_collect_none_without_start_event() {
        let mut server = Metrics::default();
        let mut state = HashMap::new();
        insert_event(&mut state, "1", EventName::Decided, u64::MAX);

        server.state = Arc::new(Mutex::new(state));
        let report = server.collect().await;
        assert!(report.is_none());
    }

    #[tokio::test]
    async fn collect() {
        let mut server = Metrics::default();
        let mut state = HashMap::new();

        insert_event(&mut state, "1", EventName::Started, nanos(1));
        insert_event(&mut state, "1", EventName::CandidateReceived, nanos(2));
        insert_event(&mut state, "1", EventName::CandidateEnqueuedInAgent, nanos(3));
        insert_event(&mut state, "1", EventName::CandidatePublishTaskStarting, nanos(4));
        insert_event(&mut state, "1", EventName::CandidatePublishStarted, nanos(5));
        insert_event(&mut state, "1", EventName::CandidatePublished, nanos(6));
        insert_event(&mut state, "1", EventName::CandidateReceivedByTalos, nanos(7));
        insert_event(&mut state, "1", EventName::Decided, nanos(7));
        insert_event(&mut state, "1", EventName::DecisionReceived, nanos(8));
        insert_event(&mut state, "1", EventName::Finished, nanos(9));

        insert_event(&mut state, "2", EventName::Started, nanos(2));
        insert_event(&mut state, "2", EventName::CandidateReceived, nanos(2));
        insert_event(&mut state, "2", EventName::CandidateEnqueuedInAgent, nanos(3));
        insert_event(&mut state, "2", EventName::CandidatePublishTaskStarting, nanos(4));
        insert_event(&mut state, "2", EventName::CandidatePublishStarted, nanos(5));
        insert_event(&mut state, "2", EventName::CandidatePublished, nanos(6));
        insert_event(&mut state, "2", EventName::CandidateReceivedByTalos, nanos(7));
        insert_event(&mut state, "2", EventName::Decided, nanos(7));
        insert_event(&mut state, "2", EventName::DecisionReceived, nanos(7));
        insert_event(&mut state, "2", EventName::Finished, nanos(7));

        server.state = Arc::new(Mutex::new(state));
        let maybe_report = server.collect().await;
        assert!(maybe_report.is_some());

        let report = maybe_report.unwrap();
        assert_eq!(report.count, 2_u64);
        assert_eq!(report.times.len(), 2);
        assert_eq!(report.total.p99.value, 8_000_000_f32);
        assert_eq!(report.total.p95.value, 8_000_000_f32);
        assert_eq!(report.total.p90.value, 8_000_000_f32);
        assert_eq!(report.total.p75.value, 8_000_000_f32);
        assert_eq!(report.total.p50.value, 5_000_000_f32);
        assert_eq!(report.publish_rate, 2_f64);
        assert_eq!(report.certify_min.total.as_millis(), millis(5));
        assert_eq!(report.certify_max.total.as_millis(), millis(8));
    }
}
// $coverage:ignore-end
