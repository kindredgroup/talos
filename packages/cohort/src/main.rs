use std::env;
// $coverage:ignore-start
use cohort::config_loader::ConfigLoader;
use cohort::core::Cohort;
use cohort::executors::random_payload::RandomPayloadExecutor;
use cohort::executors::simple_csv::SimpleCsvExecutor;
use cohort::model::bank_account::BankAccount;
use cohort::model::snapshot::Snapshot;
use cohort::state::postgres::data_store::DataStore;
use cohort::state::postgres::database::Database;

use std::sync::Arc;
use std::time::Duration;

use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::signal;

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::builder().format_timestamp_millis().init();

    let (workload_duration, transactions, accounts_file) = get_params().await;

    let (cfg_agent, cfg_kafka, cfg_db) = ConfigLoader::load()?;
    tokio::spawn(async move {
        let agent = Cohort::init_agent(cfg_agent, cfg_kafka).await;

        let database = Database::init_db(cfg_db.clone()).await;

        prefill_db(accounts_file, Arc::clone(&database)).await;

        tokio::time::sleep(Duration::from_secs(3)).await;

        log::info!("Cohort started...");

        if let Some(duration) = workload_duration {
            log::info!("Cohort workflow generator will run for: {}sec", duration);
            if let Err(e) = RandomPayloadExecutor::execute(Arc::new(agent), Arc::clone(&database), duration).await {
                log::error!("Error when generating a test load: {}", e)
            } else {
                log::info!("No more data to generate...")
            }
        } else if let Some(csv) = transactions {
            log::warn!("Cohort workflow generator will use CSV transactions: {}", csv.lines().count() - 1);
            if let Err(e) = SimpleCsvExecutor::execute(Arc::new(agent), Arc::clone(&database), csv).await {
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

async fn prefill_db(accounts_file: Option<String>, db: Arc<Database>) {
    let path = accounts_file.unwrap_or_else(|| "initial_state_accounts.json".into());

    let file = std::fs::File::open(path).unwrap();
    let reader = std::io::BufReader::new(file);

    let accounts: Vec<BankAccount> = serde_json::from_reader(reader)
        .map_err(|e| {
            log::error!("Unable to read initial data: {}", e);
        })
        .unwrap();

    let snapshot: Snapshot = serde_json::from_str(include_str!("initial_state_snapshot.json"))
        .map_err(|e| {
            log::error!("Unable to read initial data: {}", e);
        })
        .unwrap();

    log::warn!("----------------------------------");
    log::warn!("Initial state is loaded from files");
    for a in accounts.iter() {
        log::warn!("{}", a);
    }
    log::warn!("{}", snapshot);

    // Init database ...
    let updated_accounts = DataStore::prefill_accounts(Arc::clone(&db), accounts.clone()).await.unwrap();
    let updated_snapshot = DataStore::prefill_snapshot(Arc::clone(&db), snapshot.clone()).await.unwrap();

    log::warn!("----------------------------------");
    log::warn!("Current initial state");
    for a in updated_accounts.iter() {
        log::warn!("{}", a);
    }
    log::warn!("{}", updated_snapshot);
}

async fn get_params() -> (Option<u32>, Option<String>, Option<String>) {
    let args: Vec<String> = env::args().collect();
    let mut workload_duration: Option<u32> = None;
    let mut initial_state_accounts: Option<String> = None;
    let mut transactions: Option<String> = None;

    if args.len() >= 3 {
        let mut i = 1;
        while i < args.len() {
            let param_name = &args[i];
            log::warn!("{} parsing... {} from {:?}", i, param_name, args);
            if param_name.eq("--workload-duration") {
                let param_value = &args[i + 1];
                workload_duration = Some(param_value.parse().unwrap());
            } else if param_name.eq("--initial-state-accounts") {
                let param_value = &args[i + 1];
                initial_state_accounts = Some((*param_value).clone());
            } else if param_name.eq("--transactions") {
                let param_value = &args[i + 1];
                let mut file = File::open(param_value).await.unwrap();
                let mut content = String::from("");
                let _ = file.read_to_string(&mut content).await;
                log::info!("\n{}\n", content.clone());
                transactions = Some(content.replace(['\t', ' '], ""));
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

    (workload_duration, transactions, initial_state_accounts)
}

// $coverage:ignore-end
