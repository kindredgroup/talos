use opentelemetry::global;
use opentelemetry::trace::{TraceError, TracerProvider as _};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use strum::Display;
use thiserror::Error as ThisError;
use tracing::subscriber::{set_global_default, SetGlobalDefaultError};
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{fmt, EnvFilter};

pub fn init_otel_logs_tracing(name: String, enable_tracing: bool, grpc_endpoint: Option<String>, default_level: &'static str) -> Result<(), OtelInitError> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level));

    let layer_fmt = fmt::Layer::new().json();

    if !enable_tracing {
        // setup only logging
        let subscriber = tracing_subscriber::registry().with(layer_fmt).with(env_filter);

        set_global_default(subscriber).map_err(OtelInitError::from_global_subscriber_error)?;
        return Ok(());
    }

    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

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
        let subscriber = tracing_subscriber::registry().with(layer_fmt).with(env_filter).with(otlp_layer);

        set_global_default(subscriber).map_err(OtelInitError::from_global_subscriber_error)?;
    } else {
        let layer_fmt = BunyanFormattingLayer::new(name.clone(), std::io::stdout);
        let subscriber = tracing_subscriber::registry().with(env_filter).with(JsonStorageLayer).with(layer_fmt);

        set_global_default(subscriber).map_err(OtelInitError::from_global_subscriber_error)?;
    }

    tracing::info!("OTEL logging and tracing initialised");

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
