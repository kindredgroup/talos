use std::{env, sync::Arc};

use cohort::{
    config_loader::ConfigLoader,
    model::{bank_account::BankAccount, snapshot::Snapshot},
    state::postgres::{data_store::DataStore, database::Database},
};

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::builder().format_timestamp_millis().init();
    let (snapshots_file, accounts_file) = get_params().await?;

    let cfg_db = ConfigLoader::load_db_config()?;

    let database = Database::init_db(cfg_db.clone()).await.map_err(|e| e.to_string())?;

    prefill_db(snapshots_file, accounts_file, Arc::clone(&database)).await;

    Ok(())
}

async fn prefill_db(snapshots_file: String, accounts_file: String, db: Arc<Database>) {
    let data = tokio::fs::read_to_string(&accounts_file).await.unwrap();
    let accounts: Vec<BankAccount> = serde_json::from_str::<Vec<BankAccount>>(&data)
        .map_err(|e| {
            log::error!("Unable to read initial data for accounts: {}", e);
        })
        .unwrap();

    let data = std::fs::read_to_string(&snapshots_file).unwrap();
    let snapshot: Snapshot = serde_json::from_str(&data)
        .map_err(|e| {
            log::error!("Unable to read initial data for snapshots: {}", e);
        })
        .unwrap();

    // Init database ...
    let updated_accounts = DataStore::prefill_accounts(Arc::clone(&db), accounts.clone()).await.unwrap();
    let updated_snapshot = DataStore::prefill_snapshot(Arc::clone(&db), snapshot.clone()).await.unwrap();

    log::info!("----------------------------------");
    log::info!("Initial state is loaded into DB files");
    log::info!("{}", updated_snapshot);
    log::info!("Accounts: {}", updated_accounts.len());
    log::info!("----------------------------------");
}

async fn get_params() -> Result<(String, String), String> {
    let args: Vec<String> = env::args().collect();
    let mut accounts_file: Option<String> = None;
    let mut snapshot_file: Option<String> = None;

    if args.len() >= 3 {
        let mut i = 1;
        while i < args.len() {
            let param_name = &args[i];

            if param_name.eq("--accounts") {
                let param_value = &args[i + 1];
                accounts_file = Some((*param_value).clone());
            } else if param_name.eq("--snapshot") {
                let param_value = &args[i + 1];
                snapshot_file = Some((*param_value).clone());
            }

            i += 2;
        }
    } else {
        log::warn!("Missing paramters:\n  --snapshot 'path-to-json'\n  --accounts 'path-to-json'")
    }

    Ok((snapshot_file.unwrap(), accounts_file.unwrap()))
}
