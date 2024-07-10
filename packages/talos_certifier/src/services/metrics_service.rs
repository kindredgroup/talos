use std::time::Duration;

use ahash::AHashMap;
use async_trait::async_trait;
use hdrhistogram::Histogram;
use log::error;
use tokio::sync::mpsc;

use crate::core::{ServiceResult, SystemService};

pub enum MetricsServiceMessage {
    Record(String, u64),
    // PrintPercentile(String, f32),
    PrintPercentiles(String),
}

/// Service used to capture metrics from other services within talos certifier
pub struct MetricsService {
    pub metrics_rx: mpsc::Receiver<MetricsServiceMessage>,
    pub histograms: ahash::AHashMap<String, Histogram<u64>>,
}

impl MetricsService {
    pub fn new(metrics_rx: mpsc::Receiver<MetricsServiceMessage>) -> Self {
        Self {
            metrics_rx,
            histograms: AHashMap::new(),
        }
    }

    fn create_metric(&mut self, metric_name: &str) {
        self.histograms.insert(metric_name.to_string(), Histogram::new(3).unwrap());
    }

    pub fn record_metric(&mut self, metric_name: &str, value: u64) {
        if !self.histograms.contains_key(&metric_name.to_owned()) {
            self.create_metric(&metric_name)
        }

        let met = self.histograms.get_mut(&metric_name.to_string()).unwrap();
        let _ = met.record(value);

        // error!("Metric name {metric_name} and recorded value = {value}")
    }

    pub fn get_histogram(&self, metric_name: &String) -> Option<&Histogram<u64>> {
        self.histograms.get(&metric_name.to_string())
    }

    // pub fn print_metric_at_percentile(&self, metric_name: String, percentile: f32) -> u64 {
    //     let metric = self.histograms.get(&metric_name).unwrap();
    //     metric.value_at_percentile(percentile.into())
    // }
}

// Create a HDR
// Capture values for specific type
// Print values

#[async_trait]
impl SystemService for MetricsService {
    async fn run(&mut self) -> ServiceResult {
        let mut interval = tokio::time::interval(Duration::from_millis(2 * 60 * 1000));

        loop {
            tokio::select! {
                res = self.metrics_rx.recv() => {
                    if let Some(channel_msg) = res {
                        match channel_msg {
                            MetricsServiceMessage::Record(metric_name, value) => {
                                // error!("Recording for {metric_name} with value {value}");
                                self.record_metric(&metric_name, value);

                            }
                            MetricsServiceMessage::PrintPercentiles(metric_name) => {

                                if let Some(histogram) = self.get_histogram(&metric_name) {
                                    error!("++ Printing metrics for {metric_name}");
                                    error!("+++ P50     : {}", histogram.value_at_percentile(0.5));
                                    error!("+++ P90     : {}", histogram.value_at_percentile(0.9));
                                    error!("+++ P95     : {}", histogram.value_at_percentile(0.95));
                                    error!("+++ P99     : {}", histogram.value_at_percentile(0.99));
                                    error!("+++ P99.5   : {}", histogram.value_at_percentile(0.995));
                                    error!("+++ P100    : {}", histogram.value_at_percentile(1.0));
                                    error!("++----------------------------------------------++");
                                };

                            }
                        }
                    }
                }
                _ = interval.tick() => {
                    error!("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
                    error!("+++                 PRINTING STATS                          +++");
                    error!("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
                    error!("\n{}", format!("{:50} | {:^5} | {:^5} | {:^5} | {:^5} | {:^5} | {:^5} | {:^5} | {:^8}","Name","P0","P50","P90","P95","P99","P99.5","P100","Count"));
                    for (key, histogram) in &self.histograms {
                        error!("\n{}", format!("{:50} | {:^5} | {:^5} | {:^5} | {:^5} | {:^5} | {:^5} | {:^5} | {:^8}",
                        key,
                        histogram.value_at_quantile(0.0),
                        histogram.value_at_quantile(0.5),
                        histogram.value_at_quantile(0.9),
                        histogram.value_at_quantile(0.95),
                        histogram.value_at_quantile(0.99),
                        histogram.value_at_quantile(0.995),
                        histogram.value_at_quantile(1.0),
                        histogram.len())
                    );
                        // error!("++ Printing metrics for  ||| {} |||", key);
                        // error!("+++ P0      : {}", histogram.value_at_quantile(0.0));
                        // error!("+++ P50     : {}", histogram.value_at_quantile(0.5));
                        // error!("+++ P90     : {}", histogram.value_at_quantile(0.9));
                        // error!("+++ P95     : {}", histogram.value_at_quantile(0.95));
                        // error!("+++ P99     : {}", histogram.value_at_quantile(0.99));
                        // error!("+++ P99.5   : {}", histogram.value_at_quantile(0.995));
                        // error!("+++ P100    : {}", histogram.value_at_quantile(1.0));
                        // error!("+++ Count   : {}", histogram.len());
                        // error!("++----------------------------------------------++");
                    };

                }

            }
        }

        Ok(())
    }
}
