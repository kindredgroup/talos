use core::fmt;
use std::{
    borrow::Cow,
    sync::{Arc, RwLock},
};

use once_cell::sync::Lazy;

use opentelemetry_api::{
    metrics::{self, Meter, MeterProvider},
    KeyValue,
};

use super::scaling::ScalingConfig;

///
/// This module is the copy of opentelemetry_api::global, with the following changes:
/// - set_meter_provider(T) is changed to set_meter_provider(Arc<T>)
/// - set_scaling_config(...) - added new method to hold scaling configuration.
///

/// The global `Meter` provider singleton.
static GLOBAL_METER_PROVIDER: Lazy<RwLock<GlobalMeterProvider>> = Lazy::new(|| RwLock::new(GlobalMeterProvider::new(metrics::noop::NoopMeterProvider::new())));

static GLOBAL_SCALING_CONFIG: Lazy<RwLock<Arc<ScalingConfig>>> = Lazy::new(|| RwLock::new(Arc::new(ScalingConfig::default())));

pub trait ObjectSafeMeterProvider {
    fn versioned_meter_cow(
        &self,
        name: Cow<'static, str>,
        version: Option<Cow<'static, str>>,
        schema_url: Option<Cow<'static, str>>,
        attributes: Option<Vec<KeyValue>>,
    ) -> Meter;
}

impl<P> ObjectSafeMeterProvider for P
where
    P: MeterProvider,
{
    /// Return a versioned boxed tracer
    fn versioned_meter_cow(
        &self,
        name: Cow<'static, str>,
        version: Option<Cow<'static, str>>,
        schema_url: Option<Cow<'static, str>>,
        attributes: Option<Vec<KeyValue>>,
    ) -> Meter {
        self.versioned_meter(name, version, schema_url, attributes)
    }
}

#[derive(Clone)]
pub struct GlobalMeterProvider {
    provider: Arc<dyn ObjectSafeMeterProvider + Send + Sync>,
}

impl fmt::Debug for GlobalMeterProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GlobalMeterProvider").finish()
    }
}

impl MeterProvider for GlobalMeterProvider {
    fn versioned_meter(
        &self,
        name: impl Into<Cow<'static, str>>,
        version: Option<impl Into<Cow<'static, str>>>,
        schema_url: Option<impl Into<Cow<'static, str>>>,
        attributes: Option<Vec<KeyValue>>,
    ) -> Meter {
        self.provider
            .versioned_meter_cow(name.into(), version.map(Into::into), schema_url.map(Into::into), attributes)
    }
}

impl GlobalMeterProvider {
    /// Create a new global meter provider
    pub fn new<P>(provider: P) -> Self
    where
        P: MeterProvider + Send + Sync + 'static,
    {
        GlobalMeterProvider { provider: Arc::new(provider) }
    }
}

pub fn set_scaling_config(new_config: ScalingConfig) {
    let mut cfg = GLOBAL_SCALING_CONFIG.write().expect("GLOBAL_SCALING_CONFIG RwLock poisoned");
    *cfg = Arc::new(new_config);
}

pub fn set_meter_provider<P>(new_provider: P)
where
    P: metrics::MeterProvider + Send + Sync + 'static,
{
    let mut global_provider = GLOBAL_METER_PROVIDER.write().expect("GLOBAL_METER_PROVIDER RwLock poisoned");
    *global_provider = GlobalMeterProvider::new(new_provider);
}

pub fn meter_provider() -> GlobalMeterProvider {
    GLOBAL_METER_PROVIDER.read().expect("GLOBAL_METER_PROVIDER RwLock poisoned").clone()
}

pub fn scaling_config() -> Arc<ScalingConfig> {
    GLOBAL_SCALING_CONFIG.read().expect("GLOBAL_SCALING_CONFIG RwLock poisoned").clone()
}

pub fn meter(name: impl Into<Cow<'static, str>>) -> Meter {
    meter_provider().meter(name.into())
}

pub fn meter_with_version(
    name: impl Into<Cow<'static, str>>,
    version: Option<impl Into<Cow<'static, str>>>,
    schema_url: Option<impl Into<Cow<'static, str>>>,
    attributes: Option<Vec<KeyValue>>,
) -> Meter {
    meter_provider().versioned_meter(name.into(), version.map(Into::into), schema_url.map(Into::into), attributes)
}
