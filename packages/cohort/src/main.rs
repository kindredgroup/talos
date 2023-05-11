use std::env;
// $coverage:ignore-start
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::signal;

use cohort::config_loader::ConfigLoader;
use cohort::core::Cohort;
use cohort::model::bank_account::BankAccount;
use cohort::model::snapshot::Snapshot;
use cohort::state::postgres::data_store::DataStore;
use cohort::state::postgres::database::Database;

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::builder().format_timestamp_millis().init();

    let (workload_duration, transactions) = get_params().await;

    let (cfg_agent, cfg_kafka, cfg_db) = ConfigLoader::load()?;
    tokio::spawn(async move {
        let agent = Cohort::init_agent(cfg_agent, cfg_kafka).await;

        let database = Database::init_db(cfg_db.clone()).await;
        let cohort = Cohort::new(agent, Arc::clone(&database));

        prefill_db(database).await;
        log::info!("Cohort started...");

        if let Some(duration) = workload_duration {
            log::info!("Cohort workflow generator will run for: {}sec", duration);
            if let Err(e) = cohort.generate_workload(duration).await {
                log::error!("Error when generating a test load: {}", e)
            } else {
                log::info!("No more data to generate...")
            }
        }

        if let Some(csv) = transactions {
            log::info!("Cohort workflow generator will use CSV transactions: {}", csv.lines().count());
            if let Err(e) = cohort.execute_workload(csv).await {
                log::error!("Error when generating a test CSV load: {}", e)
            } else {
                log::info!("No more data to generate...")
            }
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

async fn get_params() -> (Option<u32>, Option<String>) {
    let args: Vec<String> = env::args().collect();
    let mut workload_duration: Option<u32> = None;
    let mut transactions: Option<String> = None;

    if args.len() >= 3 {
        let mut i = 1;
        while i < args.len() {
            let param_name = &args[i];
            if param_name.eq("--workflow-duration") {
                let param_value = &args[i + 1];
                workload_duration = Some(param_value.parse().unwrap());
                break;
            }

            if param_name.eq("--transactions") {
                let param_value = &args[i + 1];
                let mut file = File::open(param_value).await.unwrap();
                let mut content = String::from("");
                let _ = file.read_to_string(&mut content).await;
                transactions = Some(content);
                break;
            }

            i += 2;
        }
    }

    workload_duration = match workload_duration {
        None => {
            // check env variable
            if let Ok(value) = env::var("COHORT_WORKLOAD_DURATION") {
                Some(value.parse().unwrap())
            } else {
                None
            }
        }

        Some(value) => {
            if value > 5 * 60 * 60 {
                panic!("Please specify the duration for Cohort workflow generator. It shoiuld be no longer than 5 hours. Current value is {value} seconds.");
            }

            Some(value)
        }
    };

    (workload_duration, transactions)
}

// $coverage:ignore-end
