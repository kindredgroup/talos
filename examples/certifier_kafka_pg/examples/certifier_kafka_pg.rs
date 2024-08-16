use log::{error, info};
use opentelemetry::{global, trace::TracerProvider as _, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    trace::{Config, TracerProvider},
    Resource,
};
use std::io::Write;
use talos_certifier_adapters::{certifier_with_kafka_pg, Configuration, PgConfig, TalosCertifierChannelBuffers};
use talos_common_utils::env_var;
use talos_rdkafka_utils::kafka_config::KafkaConfig;
use talos_suffix::core::SuffixConfig;
use tokio::signal;
use tracing::{info_span, span};
use tracing_subscriber::{fmt::format, prelude::*};

struct MockConfig {
    db_mock: bool,
    certifier_mock: bool,
}

#[tokio::main]
async fn main() -> Result<(), impl std::error::Error> {
    // env_logger::builder()
    //     .format(|buf, record| writeln!(buf, "{} - {}", record.level(), record.args()))
    //     .init();
    // tracing_subscriber::fmt().pretty().init();

    let fmt_layer = tracing_subscriber::fmt::layer().event_format(format().pretty());

    let provider = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter().tonic(), // .with_endpoint("http://localhost:4317")
        )
        .with_trace_config(Config::default().with_resource(Resource::new(vec![KeyValue::new(
            opentelemetry_semantic_conventions::resource::SERVICE_NAME,
            "talos-certifier",
        )])))
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .expect("Couldn't create OTLP tracer");

    let tracer = provider.tracer_builder("talos-certifier").build();
    // global::set_tracer_provider(provider.clone());

    let telemetry_layer = tracing_opentelemetry::OpenTelemetryLayer::new(tracer);

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(fmt_layer)
        .with(telemetry_layer)
        .init();

    let span = info_span!("certifier::main");
    let _g = span.enter();

    info!("Talos certifier starting...");

    let span2 = info_span!("certifier::main", "certifier::main::TWO");
    let _g2 = span2.enter();

    info!("Talos certifier starting TWO...");

    let kafka_config = KafkaConfig::from_env(None);
    // kafka_config.extend(None, None);

    let pg_config = PgConfig::from_env();
    let mock_config = get_mock_config();
    let suffix_config = Some(SuffixConfig {
        capacity: 400_000,
        prune_start_threshold: Some(300_000),
        min_size_after_prune: Some(250_000),
    });

    let configuration = Configuration {
        suffix_config,
        certifier_mock: mock_config.certifier_mock,
        pg_config: Some(pg_config),
        kafka_config,
        db_mock: mock_config.db_mock,
    };

    let talos_certifier = certifier_with_kafka_pg(TalosCertifierChannelBuffers::default(), configuration).await?;

    // Services thread thread spawned
    let svc_handle = tokio::spawn(async move { talos_certifier.run().await });

    // Look for termination signals
    tokio::select! {
        // Talos certifier handle
        res = svc_handle => {
            if let Err(error) = res.unwrap() {
                error!("Talos Certifier shutting down due to error!!!");
                 return Err(error);
            }
        }
        // CTRL + C termination signal
        _ = signal::ctrl_c() => {
            info!("CTRL + C TERMINATION!!!!");
        },
    };

    info!("Talos Certifier shutdown complete!!!");

    Ok(())
}

fn get_mock_config() -> MockConfig {
    MockConfig {
        db_mock: env_var!("DB_MOCK").parse().unwrap(),
        certifier_mock: env_var!("CERTIFIER_MOCK").parse().unwrap(),
    }
}
