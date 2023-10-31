use std::collections::HashMap;

/// The config for metrics scaling as map where metric name is mapped to its scaling factor.
#[derive(Default)]
pub struct ScalingConfig {
    pub ratios: HashMap<String, f32>,
}

impl ScalingConfig {
    pub fn get_scale_factor(&self, metric_name: &str) -> f32 {
        *self.ratios.get(metric_name).unwrap_or(&1_f32)
    }
}
