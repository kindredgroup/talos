use log::{error, info};

use talos_suffix::core::SuffixConfig;
use talos_certifier_adapters::{certifier_with_kafka_pg, Configuration, KafkaConfig, PgConfig, TalosCertifierChannelBuffers};
use tokio::signal;

use logger::logs;

#[tokio::main]
async fn main() -> Result<(), impl std::error::Error> {
    logs::init();

    info!("Talos certifier starting...");

    let kafka_config = KafkaConfig::from_env();
    let pg_config = PgConfig::from_env();
    let suffix_config = Some(SuffixConfig {
        capacity: 30,
        prune_start_threshold: Some(25),
        min_size_after_prune: Some(10),
    });

    let configuration = Configuration {
        suffix_config,
        certifier_mock: false,
        pg_config: Some(pg_config),
        kafka_config,
        db_mock: false,
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
