use std::{env, sync::Arc};

use banking_common::state::postgres::{
    database::{Database, DatabaseError, SNAPSHOT_SINGLETON_ROW_ID},
    database_config::DatabaseConfig,
};
use cohort_banking::model::{bank_account::BankAccount, snapshot::Snapshot};
use rust_decimal::Decimal;
use tokio_postgres::Row;

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::builder().format_timestamp_millis().init();

    let accounts_file = get_params().await?;

    let cfg_db = DatabaseConfig::from_env(Some("COHORT"))?;

    let database = Database::init_db(cfg_db).await.map_err(|e| e.to_string())?;

    prefill_db(accounts_file, Arc::clone(&database)).await;

    Ok(())
}

async fn prefill_db(accounts_file: String, db: Arc<Database>) {
    let data = tokio::fs::read_to_string(&accounts_file).await.unwrap();
    let accounts: Vec<BankAccount> = serde_json::from_str::<Vec<BankAccount>>(&data)
        .map_err(|e| {
            log::error!("Unable to read initial data for accounts: {}", e);
        })
        .unwrap();

    // Init database ...
    let updated_accounts = prefill_accounts(Arc::clone(&db), accounts.clone())
        .await
        .expect("Unable to prefill accounts table");
    let updated_snapshot = prefill_snapshot(Arc::clone(&db), Snapshot { version: 0 })
        .await
        .expect("Unable to prefil snapshot table");

    log::info!("----------------------------------");
    log::info!("Initial state is loaded into DB files");
    log::info!("{}", updated_snapshot);
    log::info!("Accounts: {}", updated_accounts.len());
    log::info!("----------------------------------");
}

async fn prefill_snapshot(db: Arc<Database>, snapshot: Snapshot) -> Result<Snapshot, DatabaseError> {
    let rslt = db
        .query_opt(
            r#"SELECT "version" FROM cohort_snapshot WHERE id = $1 AND "version" > $2"#,
            &[&SNAPSHOT_SINGLETON_ROW_ID, &(snapshot.version as i64)],
            snapshot_from_row,
        )
        .await?;

    if let Some(snapshot) = rslt {
        Ok(snapshot)
    } else {
        db.query_one(
            r#"
                INSERT INTO cohort_snapshot ("id", "version") VALUES ($1, $2)
                ON CONFLICT(id) DO
                    UPDATE SET version = $2 RETURNING version
            "#,
            &[&SNAPSHOT_SINGLETON_ROW_ID, &(snapshot.version as i64)],
            snapshot_from_row,
        )
        .await
    }
}

async fn prefill_accounts(db: Arc<Database>, accounts: Vec<BankAccount>) -> Result<Vec<BankAccount>, DatabaseError> {
    let client = db.pool.get().await.unwrap();
    let mut updated_accounts = Vec::<BankAccount>::new();
    let frequency: u64 = (accounts.len() as f32 * 15.0 / 100.0) as u64;
    for acc in accounts.iter() {
        let updated = {
            let rslt = client
                .query_opt(
                    r#"SELECT "name", "number", "amount", "version" FROM bank_accounts WHERE "number" = $1 AND "version" >= $2"#,
                    &[&acc.number, &(acc.version as i64)],
                )
                .await
                .unwrap();

            if rslt.is_some() {
                account_from_row(&rslt.unwrap())?
            } else {
                // update db with new account data
                let updated_row = client
                    .query_one(
                        r#"
                            INSERT INTO bank_accounts("name", "number", "amount", "version") VALUES ($1, $2, $3, $4)
                            ON CONFLICT(number) DO
                                UPDATE SET "name" = $1, "amount" = $3, "version" = $4 RETURNING "name", "number", "amount", "version"
                        "#,
                        &[&acc.name, &acc.number, &acc.balance, &(acc.version as i64)],
                    )
                    .await
                    .unwrap();

                account_from_row(&updated_row)?
            }
        };

        if updated_accounts.len() as f32 % frequency as f32 == 0.0 {
            log::warn!("Inserted: {} accounts of {}", updated_accounts.len(), accounts.len());
        }

        updated_accounts.push(updated);
    }

    Ok(updated_accounts)
}

fn account_from_row(row: &Row) -> Result<BankAccount, DatabaseError> {
    Ok(BankAccount {
        name: row
            .try_get::<&str, String>("name")
            .map_err(|e| DatabaseError::deserialise_payload(e.to_string(), "Cannot read account name".into()))?,
        number: row
            .try_get::<&str, String>("number")
            .map_err(|e| DatabaseError::deserialise_payload(e.to_string(), "Cannot read account number".into()))?,
        version: row
            .try_get::<&str, i64>("version")
            .map_err(|e| DatabaseError::deserialise_payload(e.to_string(), "Cannot read account version".into()))? as u64,
        balance: row
            .try_get::<&str, Decimal>("amount")
            .map_err(|e| DatabaseError::deserialise_payload(e.to_string(), "Cannot read account amount".into()))?,
    })
}

fn snapshot_from_row(row: &Row) -> Result<Snapshot, DatabaseError> {
    let updated = row
        .try_get::<&str, i64>("version")
        .map_err(|e| DatabaseError::deserialise_payload(e.to_string(), "Cannot read snapshot".into()))?;

    Ok(Snapshot { version: updated as u64 })
}

async fn get_params() -> Result<String, String> {
    let args: Vec<String> = env::args().collect();
    let mut accounts_file: Option<String> = None;

    if args.len() >= 3 {
        let mut i = 1;
        while i < args.len() {
            let param_name = &args[i];

            if param_name.eq("--accounts") {
                let param_value = &args[i + 1];
                accounts_file = Some((*param_value).clone());
            }

            i += 2;
        }
    } else {
        log::warn!("Missing paramters:\n  --accounts 'path-to-json'")
    }

    Ok(accounts_file.unwrap())
}
