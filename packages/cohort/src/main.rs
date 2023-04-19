// $coverage:ignore-start
use std::sync::Arc;
use tokio::signal;

use cohort::config_loader::ConfigLoader;
use cohort::core::Cohort;
use cohort::state::postgres::database::Database;

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::builder().format_timestamp_millis().init();

    let (cfg_agent, cfg_kafka, cfg_db) = ConfigLoader::load()?;
    tokio::spawn(async move {
        let agent = Cohort::init_agent(cfg_agent, cfg_kafka).await;

        let database = Database::init_db(cfg_db).await;
        let mut cohort = Cohort::new(agent, Arc::clone(&database));

        cohort.start().await;
        log::info!("Cohort started...");

        if let Err(e) = cohort.generate_workload(10).await {
            log::error!("Error when generating a test load: {}", e)
        } else {
            log::info!("No more data to generate...")
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
// $coverage:ignore-end
