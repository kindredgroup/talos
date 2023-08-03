use std::collections::HashSet;

use opentelemetry_stdout::MetricsData;

use super::{
    global,
    model::{Attribute, MetricsDataContainer},
};

pub struct MetricsToStringPrinter {
    pub print_raw_histogram: bool,
    parallel_threads: u64,
    resource_filter: Option<HashSet<Attribute>>,
    scope_filter: Option<HashSet<&'static str>>,
    metrics_filter: Option<HashSet<&'static str>>,
}

/// Prints metrics as formatted text
impl MetricsToStringPrinter {
    pub fn new(threads: u64, print_raw_histogram: bool) -> Self {
        Self {
            print_raw_histogram,
            parallel_threads: threads,
            resource_filter: None,
            scope_filter: None,
            metrics_filter: None,
        }
    }

    pub fn new_with_filters(
        threads: u64,
        resource_filter: Option<HashSet<Attribute>>,
        scope_filter: Option<HashSet<&'static str>>,
        metrics_filter: Option<HashSet<&'static str>>,
    ) -> Self {
        Self {
            print_raw_histogram: false,
            parallel_threads: threads,
            resource_filter,
            scope_filter,
            metrics_filter,
        }
    }

    #[allow(clippy::single_char_add_str)]
    pub fn print(&self, metrics: &MetricsData) -> Result<String, String> {
        let mut out: String = "".to_owned();
        let serde_value = serde_json::to_value(metrics).map_err(|e| e.to_string())?;
        let container = serde_json::from_value::<MetricsDataContainer>(serde_value).map_err(|e| e.to_string())?;
        let percentile_labels = vec![25.0, 50.0, 75.0, 90.0, 95.0, 98.0, 99.0, 99.9, 99.99];

        if let Some(ref filter) = self.resource_filter {
            let mut filter_passed = filter.is_empty();
            for attr in filter.iter() {
                if container.resource_metrics.resource.attributes.iter().any(|a| a.eq(attr)) {
                    filter_passed = true;
                    break;
                }
            }
            if !filter_passed {
                return Ok("".into());
            }
        }

        for scope_metrics in container.resource_metrics.scope_metrics.iter() {
            if let Some(ref filter) = self.scope_filter {
                let filter_passed = filter.is_empty() || filter.contains(scope_metrics.scope.name.as_str());
                if !filter_passed {
                    continue;
                }
            }

            for metric in scope_metrics.metrics.iter() {
                // apply metric name filter
                if let Some(ref filter) = self.metrics_filter {
                    let filter_passed = filter.is_empty() || filter.contains(metric.name.as_str());
                    if !filter_passed {
                        continue;
                    }
                }

                if let Some(hist_c) = &metric.histogram {
                    let hist = hist_c.histogram();
                    let buckets = hist.compact();
                    let scale_factor = global::scaling_config().get_scale_factor(metric.name.as_str());

                    out.push_str("\n---------------------------------------");
                    out.push_str(format!("\nHistogram: \"{}\"\n", metric.name).as_str());

                    if self.print_raw_histogram {
                        for bucket in buckets.iter() {
                            if bucket.count == 0 {
                                continue;
                            }
                            out.push_str(
                                format!(
                                    "\n{:>5.0}: {:>7} | {:>7} | {:>5.2}%",
                                    bucket.label / scale_factor as f64,
                                    bucket.count,
                                    bucket.sum,
                                    bucket.percentage
                                )
                                .as_str(),
                            );
                        }
                    }

                    let mut bucket_index = 0_usize;
                    for percentile_label in percentile_labels.iter() {
                        for bucket in buckets.iter().skip(bucket_index) {
                            bucket_index += 1;
                            if *percentile_label <= bucket.percentage {
                                bucket_index -= 1;
                                let scaled_label = bucket.label as f32 / scale_factor;
                                out.push_str(
                                    format!(
                                        "\n{:>5.2}% | {:>7.2} {} | samples: {:>7}",
                                        percentile_label, scaled_label, metric.unit, bucket.sum
                                    )
                                    .as_str(),
                                );
                                break;
                            }
                        }
                    }

                    out.push_str("\n");
                    out.push_str(format!("\n{:>10} : {:.2} {}", "Min", hist.min / scale_factor as f64, metric.unit).as_str());
                    out.push_str(format!("\n{:>10} : {:.2} {}", "Max", hist.max / scale_factor as f64, metric.unit).as_str());
                    out.push_str(format!("\n{:>10} : {}", "Count", hist.count).as_str());
                    if metric.is_time_unit() {
                        if let Some(duration_sec) = metric.to_seconds(hist.sum / self.parallel_threads as f64 / scale_factor as f64) {
                            out.push_str(format!("\n{:>10} : {:.2} sec", "Duration", duration_sec).as_str());
                            out.push_str(format!("\n{:>10} : {:.2} avg tps", "Throughput", hist.count as f64 / duration_sec).as_str());
                        } else {
                            out.push_str(format!("\n{:>10} : ERROR. Unable to determine time unit from this: '{}')", "Duration", metric.unit).as_str());
                            out.push_str("\nThroughput : N/A");
                        }
                    } else {
                        out.push_str(format!("\n{:>10} : {:.2} {}", "Sum", hist.sum, metric.unit).as_str());
                    }
                } else if let Some(count_container) = &metric.count {
                    out.push_str("\n---------------------------------------");
                    let counter = count_container.counter();
                    out.push_str(format!("\n{:>11} : {}", "Metric name", metric.name).as_str());
                    out.push_str(format!("\n{:>11} : {}", "Value", counter.value).as_str());
                } else {
                    out.push_str("\nNo data");
                }
            }
        }

        Ok(out)
    }
}
