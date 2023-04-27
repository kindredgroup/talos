// $coverage:ignore-start
use std::sync::Arc;
use tokio::signal;

use cohort::config_loader::ConfigLoader;
use cohort::core::Cohort;
use cohort::model::bank_account::BankAccount;
use cohort::state::model::Snapshot;
use cohort::state::postgres::data_store::DataStore;
use cohort::state::postgres::database::Database;

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::builder().format_timestamp_millis().init();

    let (cfg_agent, cfg_kafka, cfg_db) = ConfigLoader::load()?;
    tokio::spawn(async move {
        let agent = Cohort::init_agent(cfg_agent, cfg_kafka).await;

        let database = Database::init_db(cfg_db.clone()).await;
        let cohort = Cohort::new(agent, Arc::clone(&database));

        prefill_db(database).await;
        log::info!("Cohort started...");

        if let Err(e) = cohort.generate_workload(2).await {
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

async fn prefill_db(db: Arc<Database>) {
    let accounts: Vec<BankAccount> = serde_json::from_str(include_str!("initial_state_accounts.json"))
        .map_err(|e| {
            log::error!("Unable to read initial data: {}", e);
        })
        .unwrap();

    let snapshot: Snapshot = serde_json::from_str(include_str!("initial_state_snapshot.json"))
        .map_err(|e| {
            log::error!("Unable to read initial data: {}", e);
        })
        .unwrap();

    log::info!("----------------------------------");
    log::info!("Initial state is loaded from files");
    for a in accounts.iter() {
        log::info!("{}", a);
    }
    log::info!("{}", snapshot);

    // Init database ...
    let updated_accounts = DataStore::prefill_accounts(Arc::clone(&db), accounts.clone()).await.unwrap();
    let updated_snapshot = DataStore::prefill_snapshot(Arc::clone(&db), snapshot.clone()).await.unwrap();

    log::info!("----------------------------------");
    log::info!("Current initial state");
    for a in updated_accounts.iter() {
        log::info!("{}", a);
    }
    log::info!("{}", updated_snapshot);
}
// $coverage:ignore-end
