use opentelemetry_sdk::metrics::{reader::AggregationSelector, Aggregation, InstrumentKind};

use super::buckets::BUCKETS_4K as BUCKETS;

#[derive(Debug)]
pub struct CustomHistogramSelector {
    buckets: Vec<f64>,
}

impl Default for CustomHistogramSelector {
    fn default() -> Self {
        CustomHistogramSelector {
            buckets: vec![0.0, 10.0, 100.0, 500.0, 1_000.0, 10_000.0],
        }
    }
}

impl CustomHistogramSelector {
    pub fn new_with_4k_buckets() -> Result<Self, String> {
        let mut buckets: Vec<f64> = Vec::new();
        for b in BUCKETS {
            buckets.push(b as f64)
        }

        Ok(Self { buckets })
    }
    pub fn new2(csv_buckets: &str) -> Result<Self, String> {
        let buckets_iter = csv_buckets.split(',');

        let mut buckets: Vec<f64> = Vec::new();
        for txt in buckets_iter {
            let cleaned = txt.replace(' ', "");
            let parsed = cleaned
                .parse::<f64>()
                .map_err(|e| format!("Cannot convert this '{}' to f64. Error: {}", cleaned, e))?;
            buckets.push(parsed);
        }

        Ok(Self { buckets })
    }
}

impl AggregationSelector for CustomHistogramSelector {
    fn aggregation(&self, kind: InstrumentKind) -> Aggregation {
        match kind {
            InstrumentKind::Counter | InstrumentKind::UpDownCounter | InstrumentKind::ObservableCounter | InstrumentKind::ObservableUpDownCounter => {
                Aggregation::Sum
            }
            InstrumentKind::ObservableGauge => Aggregation::LastValue,
            InstrumentKind::Histogram => Aggregation::ExplicitBucketHistogram {
                boundaries: self.buckets.clone(),
                record_min_max: true,
            },
        }
    }
}
