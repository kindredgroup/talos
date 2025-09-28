use opentelemetry::global;
use opentelemetry::trace::{TraceError, TracerProvider as _};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use strum::Display;
use thiserror::Error as ThisError;
use tracing::subscriber::SetGlobalDefaultError;
use tracing_bunyan_formatter::BunyanFormattingLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter, Layer};

/// Provide the tracing layers to be used for replicator
pub fn init_log_and_otel_tracing_layers(
    name: String,
    enable_tracing: bool,
    grpc_endpoint: Option<String>,
    default_level: &'static str,
) -> Result<Box<dyn Layer<tracing_subscriber::Registry> + Send + Sync>, OtelInitError> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level));

    let layer_fmt = fmt::Layer::new().json();

    if !enable_tracing {
        return Ok(Box::new(env_filter.and_then(layer_fmt)));
    }

    if let Some(grpc_endpoint) = grpc_endpoint.clone() {
        let otel_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(grpc_endpoint)
            .with_protocol(opentelemetry_otlp::Protocol::Grpc)
            .build()
            .map_err(OtelInitError::from_exporter_error)?;

        let otlp_trace_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_batch_exporter(otel_exporter)
            .build();

        let otlp_layer = tracing_opentelemetry::layer().with_tracer(otlp_trace_provider.tracer(name.clone()));
        Ok(Box::new(env_filter.and_then(layer_fmt).and_then(otlp_layer)))
    } else {
        let layer_fmt = BunyanFormattingLayer::new(name.clone(), std::io::stdout);
        Ok(Box::new(env_filter.and_then(layer_fmt).boxed()))
    }
}

/**
 * This module is intentional duplicate with packages/cohort_sdk/otel.
 * We will externalise them into re-usable commons when we apply OTEL to full Talos ecosystem.
 */

pub fn init_otel_logs_tracing(name: String, enable_tracing: bool, grpc_endpoint: Option<String>, default_level: &'static str) -> Result<(), OtelInitError> {
    if let Ok(tracing_layers) = init_log_and_otel_tracing_layers(name, enable_tracing, grpc_endpoint, default_level) {
        if let Err(error) = tracing_subscriber::registry().with(tracing_layers).try_init() {
            tracing::debug!(
                "OTEL logging and tracing not instantiated from the library as there is already an instance of tracing_subscriber. Error = {error:?}"
            )
        } else {
            opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
            tracing::info!("OTEL logging and tracing initialised");
        }
    }

    Ok(())
}

pub fn init_otel_metrics(grpc_endpoint: Option<String>) -> Result<(), OtelInitError> {
    if let Some(grpc_endpoint) = grpc_endpoint {
        let otel_exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_endpoint(grpc_endpoint)
            .with_protocol(opentelemetry_otlp::Protocol::Grpc)
            .build()
            .map_err(|metric_error| OtelInitError {
                kind: InitErrorType::MetricError,
                reason: "Unable to initialise metrics exporter".into(),
                cause: Some(format!("{:?}", metric_error)),
            })?;

        let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_periodic_exporter(otel_exporter)
            .build();

        tracing::info!("OTEL metrics provider initialised");
        global::set_meter_provider(provider);
    }

    tracing::info!("OTEL metrics initialised");

    Ok(())
}

#[derive(Debug, ThisError)]
#[error("Error initialising OTEL telemetry: '{kind}'.\nReason: {reason}\nCause: {cause:?}")]
pub struct OtelInitError {
    pub kind: InitErrorType,
    pub reason: String,
    pub cause: Option<String>,
}

impl OtelInitError {
    pub fn from_global_subscriber_error(cause: SetGlobalDefaultError) -> Self {
        OtelInitError {
            kind: InitErrorType::GlobalScubscriberError,
            reason: "Unable to set subscriber into global OTEL registry".into(),
            cause: Some(cause.to_string()),
        }
    }

    pub fn from_exporter_error(cause: TraceError) -> Self {
        OtelInitError {
            kind: InitErrorType::SpanExporter,
            reason: "Unable to initialise OTEL exporter".into(),
            cause: Some(cause.to_string()),
        }
    }
}

#[derive(Debug, Display, PartialEq, Clone)]
pub enum InitErrorType {
    GlobalScubscriberError,
    MetricError,
    SpanExporter,
}
