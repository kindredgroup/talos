use opentelemetry::global;
use opentelemetry_otlp::WithExportConfig;
use strum::Display;
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
#[error("Error initialising OTEL telemetry: '{kind}'.\nReason: {reason}\nCause: {cause:?}")]
pub struct OtelInitError {
    pub kind: InitErrorType,
    pub reason: String,
    pub cause: Option<String>,
}

#[derive(Debug, Display, PartialEq, Clone)]
pub enum InitErrorType {
    MetricError,
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

        tracing::info!("OTEL metrics provider initialised with endpoint");
        global::set_meter_provider(provider);
    } else {
        tracing::info!("No OTEL endpoint provided, metrics will default to NoopMeterProvider");
    }

    Ok(())
}
