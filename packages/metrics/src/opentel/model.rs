use std::time::Duration;

use serde::Deserialize;

///
/// Data structures in this module are 99% compatible with opentelemetry JSON schema.
/// We just did a tiny rename where it made sense from consumer point of view.
/// This package was required to introduce a typed metrics model to our app. The Rust structs of opentelemetry model are
/// not exposed to public, they are only available as exported JSON (via serde). This data model is representation of that
/// JSON schema.
///
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricsDataContainer {
    pub resource_metrics: ResourceMetrics,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceMetrics {
    pub resource: Resource,
    pub scope_metrics: Vec<ScopeMetrics>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Resource {
    pub attributes: Vec<Attribute>,
}

#[derive(Deserialize, Eq, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Attribute {
    pub key: String,
    pub value: Option<StringValue>,
}

#[derive(Deserialize, Eq, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct StringValue {
    #[serde(alias = "stringValue")]
    pub value: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeMetrics {
    pub scope: Scope,
    pub metrics: Vec<Metric>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Scope {
    pub name: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Metric {
    pub name: String,
    pub unit: String,
    pub histogram: Option<HistogramContainer>,
    #[serde(alias = "sum")]
    pub count: Option<CountContainer>,
}

impl Metric {
    pub fn is_time_unit(&self) -> bool {
        self.unit == "millis"
            || self.unit == "ms"
            || self.unit == "sec"
            || self.unit == "s"
            || self.unit == "ns"
            || self.unit == "mcs"
            || self.unit == "μs"
            || self.unit == "h"
            || self.unit == "hr"
            || self.unit == "hrs"
    }

    pub fn to_seconds(&self, value: f64) -> Option<f64> {
        if !self.is_time_unit() {
            None
        } else if self.unit == "hrs" || self.unit == "hr" || self.unit == "h" {
            Some(value * 60_f64)
        } else if self.unit == "millis" || self.unit == "ms" {
            Some(value / 1_000_f64)
        } else if self.unit == "μs" || self.unit == "mcs" {
            Some(value / 1_000_000_f64)
        } else if self.unit == "ns" {
            Some(value / 1_000_000_000_f64)
        } else {
            Some(value)
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Counter {
    pub value: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CountContainer {
    pub data_points: Vec<Counter>,
}

impl CountContainer {
    pub fn counter(&self) -> &Counter {
        &self.data_points[0]
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistogramContainer {
    pub data_points: Vec<Histogram>,
}

impl HistogramContainer {
    pub fn histogram(&self) -> &Histogram {
        &self.data_points[0]
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Histogram {
    pub count: u64,
    pub min: f64,
    pub max: f64,
    pub sum: f64,
    pub start_time_unix_nano: u64,
    pub time_unix_nano: u64,
    #[serde(alias = "explicitBounds")]
    pub buckets: Vec<f64>,
    #[serde(alias = "bucketCounts")]
    pub counts: Vec<u64>,
}

impl Histogram {
    pub fn started(&self) -> Duration {
        Duration::from_nanos(self.start_time_unix_nano)
    }
    pub fn time(&self) -> Duration {
        Duration::from_nanos(self.time_unix_nano)
    }
    pub fn compact(&self) -> Vec<BucketData> {
        let mut hist: Vec<BucketData> = Vec::new();
        let mut sum = 0_u64;
        for (i, label) in self.buckets.iter().enumerate() {
            let count = self.counts[i];
            sum += count;
            let row = BucketData {
                label: *label,
                count,
                sum,
                percentage: (100.0 * sum as f64) / self.count as f64,
            };
            hist.push(row);
        }

        hist
    }
}

pub struct BucketData {
    pub label: f64,
    pub count: u64,
    pub sum: u64,
    pub percentage: f64,
}
