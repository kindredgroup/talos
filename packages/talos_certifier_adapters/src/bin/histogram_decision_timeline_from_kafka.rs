use std::{collections::HashMap, time::Duration};

use talos_certifier::{model::metrics::TxProcessingTimeline, ports::MessageReciever, ChannelMessage};
use talos_certifier_adapters::KafkaConsumer;
use talos_metrics::model::MinMax;
use talos_rdkafka_utils::kafka_config::KafkaConfig;
use time::OffsetDateTime;
use tokio::time::timeout;

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::builder().format_timestamp_millis().init();

    let mut kafka_config = KafkaConfig::from_env(None);
    kafka_config.extend(
        None,
        Some(HashMap::from([
            ("auto.commit.enable".into(), "false".into()),
            ("auto.offset.reset".into(), "earliest".into()),
        ])),
    );

    kafka_config.group_id = format!("talos-metric-histogram-timings-{}", uuid::Uuid::new_v4());
    let mut kafka_consumer = KafkaConsumer::new(&kafka_config);
    kafka_consumer.subscribe().await.unwrap();

    log::warn!("Aggregating timelines...");
    // collect min and max times of each span in the decision processing timeline
    let aggregates = aggregate_timelines(&mut kafka_consumer).await?;
    let total_decisions = aggregates.1;
    let progress_frequency = if total_decisions < 100 {
        10
    } else {
        // every 5%%
        (total_decisions as f32 / 100_f32 * 5_f32) as u64
    };

    kafka_consumer.unsubscribe().await;
    kafka_config.group_id = format!("talos-metric-histogram-{}", uuid::Uuid::new_v4());
    let mut kafka_consumer2 = KafkaConsumer::new(&kafka_config);

    kafka_consumer2.subscribe().await.unwrap();

    log::warn!("Computing histograms of {} decisions", total_decisions);

    let mut bukets_start_at = MinMax::default();
    bukets_start_at.add(aggregates.0.candidate_published.min);
    bukets_start_at.add(aggregates.0.candidate_received.min);
    bukets_start_at.add(aggregates.0.candidate_processing_started.min);
    bukets_start_at.add(aggregates.0.decision_created_at.min);
    bukets_start_at.add(aggregates.0.db_save_started.min);
    bukets_start_at.add(aggregates.0.db_save_ended.min);

    let earliest_bucket_start = bukets_start_at.min;

    log::warn!(
        "Earliest bucket starts at {:?}",
        OffsetDateTime::from_unix_timestamp_nanos(earliest_bucket_start)
    );

    let mut hist_candidate_published = Histogram::new(earliest_bucket_start);
    let mut hist_candidate_received = Histogram::new(earliest_bucket_start);
    let mut hist_candidate_processing_started = Histogram::new(earliest_bucket_start);
    let mut hist_decision_created_at = Histogram::new(earliest_bucket_start);
    let mut hist_db_save_started = Histogram::new(earliest_bucket_start);
    let mut hist_db_save_ended = Histogram::new(earliest_bucket_start);

    let mut item_number = 0_u64;
    loop {
        let rslt_consumed = timeout(Duration::from_secs(10), kafka_consumer2.consume_message()).await;
        let message = match rslt_consumed {
            Ok(Ok(Some(message))) => message,
            Ok(Err(_)) => continue,
            _ => break,
        };

        if let ChannelMessage::Decision(decision) = message {
            let decision_message = decision.message;
            // log::warn!("Decision {:?}", msg_decision);
            item_number += 1;
            if item_number % progress_frequency == 0 {
                log::warn!("Progress: {} of {}", item_number, total_decisions);
            }
            hist_candidate_published.include(&decision_message.metrics, decision_message.metrics.candidate_published);
            hist_candidate_received.include(&decision_message.metrics, decision_message.metrics.candidate_received);
            hist_candidate_processing_started.include(&decision_message.metrics, decision_message.metrics.candidate_processing_started);
            hist_decision_created_at.include(&decision_message.metrics, decision_message.metrics.decision_created_at);
            hist_db_save_started.include(&decision_message.metrics, decision_message.metrics.db_save_started);
            hist_db_save_ended.include(&decision_message.metrics, decision_message.metrics.db_save_ended);

            if item_number == total_decisions {
                break;
            }
        }
    }

    log::warn!("Reading of messages ended");
    kafka_consumer2.unsubscribe().await;

    let mut bucket_length = MinMax::default();
    bucket_length.add(hist_candidate_published.buckets.len() as i128);
    bucket_length.add(hist_candidate_received.buckets.len() as i128);
    bucket_length.add(hist_candidate_processing_started.buckets.len() as i128);
    bucket_length.add(hist_decision_created_at.buckets.len() as i128);
    bucket_length.add(hist_db_save_started.buckets.len() as i128);
    bucket_length.add(hist_db_save_ended.buckets.len() as i128);

    log::warn!("Histograms are ready, there are: {} 1 second buckets.", bucket_length.max);

    log::warn!("Headers: 'By Publish Time',,,,,,'-','By Receive Time',,,,,,'-','Processing Started',,,,,,'-','By Decision Created',,,,,,'-','By DB Save Started',,,,,,'-','By DB Save Ended',,,,,");
    for bucket in 1..bucket_length.max {
        log::warn!(
            "METRIC (histograms MAX): {},'-',{},'-',{},'-',{},'-',{},'-',{}",
            hist_candidate_published.to_csv_max(bucket).await,
            hist_candidate_received.to_csv_max(bucket).await,
            hist_candidate_processing_started.to_csv_max(bucket).await,
            hist_decision_created_at.to_csv_max(bucket).await,
            hist_db_save_started.to_csv_max(bucket).await,
            hist_db_save_ended.to_csv_max(bucket).await,
        );
    }
    Ok(())
}

async fn aggregate_timelines(consumer: &mut KafkaConsumer) -> Result<(TimelineAggregates, u64), String> {
    let mut total = 0_u64;
    let mut aggregates = TimelineAggregates::default();
    loop {
        let rslt_consumed = timeout(Duration::from_secs(3), consumer.consume_message()).await;
        let message = match rslt_consumed {
            Ok(Ok(Some(message))) => message,
            Ok(Err(_)) => continue,
            _ => break,
        };

        if let ChannelMessage::Decision(decision) = message {
            total += 1;
            aggregates.merge(decision.message.metrics);
        }
    }

    Ok((aggregates, total))
}

#[derive(Default)]
struct TimelineAggregates {
    pub candidate_published: MinMax,
    pub candidate_received: MinMax,
    pub candidate_processing_started: MinMax,
    pub decision_created_at: MinMax,
    pub db_save_started: MinMax,
    pub db_save_ended: MinMax,
}

impl TimelineAggregates {
    fn merge(&mut self, data: TxProcessingTimeline) {
        self.candidate_published.add(data.candidate_published);
        self.candidate_received.add(data.candidate_received);
        self.candidate_processing_started.add(data.candidate_processing_started);
        self.decision_created_at.add(data.decision_created_at);
        self.db_save_started.add(data.db_save_started);
        self.db_save_ended.add(data.db_save_ended);
    }
}

#[derive(Clone)]
struct Spans {
    pub span1_candidate_kafka_trip: MinMax,
    pub span2_talos_internal_bus: MinMax,
    pub span3_deciding: MinMax,
    pub span4_prepare_for_save: MinMax,
    pub span5_save_to_db: MinMax,
}

impl Spans {
    fn new() -> Self {
        Self {
            span1_candidate_kafka_trip: MinMax::default(),
            span2_talos_internal_bus: MinMax::default(),
            span3_deciding: MinMax::default(),
            span4_prepare_for_save: MinMax::default(),
            span5_save_to_db: MinMax::default(),
        }
    }

    fn include(&mut self, times: &TxProcessingTimeline) {
        self.span1_candidate_kafka_trip.add(times.candidate_received - times.candidate_published);
        self.span2_talos_internal_bus.add(times.candidate_processing_started - times.candidate_received);
        self.span3_deciding.add(times.decision_created_at - times.candidate_processing_started);
        self.span4_prepare_for_save.add(times.db_save_started - times.decision_created_at);
        self.span5_save_to_db.add(times.db_save_ended - times.db_save_started);
    }
}

struct Histogram {
    pub start_time: i128,
    pub buckets: Vec<(u64, Spans)>,
}

impl Histogram {
    fn new(start_time: i128) -> Self {
        Histogram {
            start_time,
            buckets: Vec::new(),
        }
    }

    fn include(&mut self, metrics: &TxProcessingTimeline, value: i128) {
        let elapsed = Duration::from_nanos((value - self.start_time) as u64);
        let bucket = elapsed.as_secs_f32().ceil() as usize;
        while self.buckets.len() < bucket + 1 {
            self.buckets.push((0, Spans::new()));
        }

        if let Some((count, spans)) = self.buckets.get_mut(bucket) {
            spans.include(metrics);
            self.buckets[bucket].0 = *count + 1;
        }
    }

    async fn get(&self, bucket: usize) -> (u64, Spans) {
        if let Some(data) = self.buckets.get(bucket) {
            (data.0, data.1.clone())
        } else {
            (0, Spans::new())
        }
    }

    async fn to_csv_max(&self, bucket: i128) -> String {
        let (count, spans) = self.get(bucket as usize).await;
        format!(
            "{},{},{},{},{},{}",
            count,
            spans.span1_candidate_kafka_trip.max,
            spans.span2_talos_internal_bus.max,
            spans.span3_deciding.max,
            spans.span4_prepare_for_save.max,
            spans.span5_save_to_db.max,
        )
    }
}
