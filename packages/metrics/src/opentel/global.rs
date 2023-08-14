use std::sync::{Arc, RwLock};

use once_cell::sync::Lazy;

use super::scaling::ScalingConfig;

static GLOBAL_SCALING_CONFIG: Lazy<RwLock<Arc<ScalingConfig>>> = Lazy::new(|| RwLock::new(Arc::new(ScalingConfig::default())));

pub fn set_scaling_config(new_config: ScalingConfig) {
    let mut cfg = GLOBAL_SCALING_CONFIG.write().expect("GLOBAL_SCALING_CONFIG RwLock poisoned");
    *cfg = Arc::new(new_config);
}

pub fn scaling_config() -> Arc<ScalingConfig> {
    GLOBAL_SCALING_CONFIG.read().expect("GLOBAL_SCALING_CONFIG RwLock poisoned").clone()
}
