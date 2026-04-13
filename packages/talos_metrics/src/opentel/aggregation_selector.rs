// use opentelemetry_sdk::metrics::{new_view, Aggregation, Instrument, InstrumentKind, Stream};

// use super::buckets::BUCKETS_4K as BUCKETS;

// #[derive(Debug, Clone)]
// pub struct CustomHistogramSelector {
//     buckets: Vec<f64>,
// }

// impl Default for CustomHistogramSelector {
//     fn default() -> Self {
//         CustomHistogramSelector {
//             buckets: vec![0.0, 10.0, 100.0, 500.0, 1_000.0, 10_000.0],
//         }
//     }
// }

// impl CustomHistogramSelector {
//     pub fn new_with_4k_buckets() -> Result<Self, String> {
//         let buckets = BUCKETS.iter().map(|&b| b as f64).collect();
//         Ok(Self { buckets })
//     }

//     pub fn new2(csv_buckets: &str) -> Result<Self, String> {
//         let buckets = csv_buckets
//             .split(',')
//             .map(|txt| {
//                 let cleaned = txt.replace(' ', "");
//                 cleaned
//                     .parse::<f64>()
//                     .map_err(|e| format!("Cannot convert this '{}' to f64. Error: {}", cleaned, e))
//             })
//             .collect::<Result<Vec<f64>, _>>()?;

//         Ok(Self { buckets })
//     }

//     pub fn into_view(self) -> Arc<dyn Fn(&Instrument) -> Option<Stream> + Send + Sync> {
//         let criteria = Instrument::new().kind(InstrumentKind::Histogram);
//         let mask = Stream::new().aggregation(Aggregation::ExplicitBucketHistogram {
//             boundaries: self.buckets,
//             record_min_max: true,
//         });
//         Arc::new(new_view(criteria, mask))
//     }
// }
