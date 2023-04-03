use tokio::signal;

use cohort::config_loader::ConfigLoader;
use cohort::core::Cohort;

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::builder().format_timestamp_millis().init();

    let (cfg_agent, cfg_kafka) = ConfigLoader::load()?;
    tokio::spawn(async move {
        let agent = Cohort::make_agent(cfg_agent, cfg_kafka).await;
        let mut cohort = Cohort::new(agent);

        cohort.start().await;
        log::info!("Cohort started...");

        if let Err(e) = cohort.generate_workload(10).await {
            log::error!("Error when generating a test load: {}", e)
        }
    });

    tokio::select! {
        // CTRL + C termination signal
        _ = signal::ctrl_c() => {
            log::info!("Shutting down");
        }
    }

    Ok(())
}
