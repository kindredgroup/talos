use log::{error, info};
use talos_certifier_adapters::{certifier_with_kafka_pg, Configuration, PgConfig, TalosCertifierChannelBuffers};
use talos_common_utils::env_var;
use talos_rdkafka_utils::kafka_config::KafkaConfig;
use talos_suffix::core::SuffixConfig;
use tokio::signal;

struct MockConfig {
    db_mock: bool,
    certifier_mock: bool,
}

#[tokio::main]
async fn main() -> Result<(), impl std::error::Error> {
    env_logger::builder().format_timestamp_millis().init();

    info!("Talos certifier starting...");

    let kafka_config = KafkaConfig::from_env(None);
    // kafka_config.extend(None, None);

    let pg_config = PgConfig::from_env();
    let mock_config = get_mock_config();
    let suffix_config = Some(SuffixConfig {
        capacity: 400_000,
        prune_start_threshold: Some(300_000),
        min_size_after_prune: Some(150_000),
    });

    let configuration = Configuration {
        suffix_config,
        certifier_mock: mock_config.certifier_mock,
        pg_config: Some(pg_config),
        kafka_config,
        db_mock: mock_config.db_mock,
        app_name: None,
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
