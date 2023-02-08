use log::{error, info};
use talos_certifier::errors::SystemServiceError;

use talos_certifier_adapters::{certifier_with_kafka_pg, TalosCertifierChannelBuffers};
use tokio::signal;

use logger::logs;

#[tokio::main]
async fn main() -> Result<(), SystemServiceError> {
    logs::init();

    info!("Talos certifier starting...");

    let talos_certifier = certifier_with_kafka_pg(TalosCertifierChannelBuffers::default()).await?;

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
