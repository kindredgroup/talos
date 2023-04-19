use std::env;

use tokio_postgres::error::SqlState;
use tokio_postgres::Client;

use cohort::config_loader::ConfigLoader;
use cohort::state::postgres::database_config::DatabaseConfig;

mod embedded {
    refinery::embed_migrations!("./src/bin/sql");
}

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::builder().format_timestamp_millis().init();

    let args: Vec<String> = env::args().collect();
    let mut should_migrate = false;
    let mut should_create_db = false;
    if args.len() == 1 {
        should_migrate = true;
    } else {
        for arg in args.iter() {
            should_migrate |= arg == "migrate-db";
            should_create_db |= arg == "create-db";
        }
    }

    if !should_create_db && !should_migrate {
        panic!("Missing arguments. Specify 'migrate-db' or 'create-db' or both.");
    }

    let cfg = ConfigLoader::load_db_config().unwrap();
    if should_create_db {
        create_db(&cfg).await?;
    }
    if should_migrate {
        migrate_schema(&cfg).await?;
    }

    Ok(())
}

async fn create_db(cfg: &DatabaseConfig) -> Result<(), String> {
    let rslt = connect(cfg.get_connection_string(&cfg.database).as_str()).await;
    if let Err(pg_error) = rslt {
        return if let Some(&SqlState::UNDEFINED_DATABASE) = pg_error.code() {
            let url = cfg.get_connection_string("");
            let client = connect(url.as_str()).await.unwrap();
            client.execute(&format!("CREATE DATABASE \"{}\" ", cfg.database), &[]).await.unwrap();
            log::info!("Created database: '{}'", &cfg.database);
            Ok(())
        } else {
            Err(format!(
                "Cannot connect to database using this url: '{}'. Error: {}",
                &cfg.get_public_connection_string(&cfg.database),
                pg_error,
            ))
        };
    }

    log::info!("Database '{}' already exists.", cfg.database);
    Ok(())
}

async fn migrate_schema(cfg: &DatabaseConfig) -> Result<(), String> {
    let url = cfg.get_connection_string(&cfg.database);
    let rslt = connect(url.as_str()).await;
    if let Err(e) = rslt {
        return Err(format!(
            "Cannot to connect to database using this url: '{}'. Error: {}",
            cfg.get_public_connection_string(&cfg.database),
            e,
        ));
    }

    let mut client = rslt.unwrap();

    let runner = embedded::migrations::runner();
    let migration_report = runner.run_async(&mut client).await.unwrap();
    for migration in migration_report.applied_migrations() {
        log::info!("Migration Applied -  Name: {}, Version: {}", migration.name(), migration.version());
    }
    log::info!("DB sql finished!");

    Ok(())
}

async fn connect(connection_string: &str) -> Result<Client, tokio_postgres::Error> {
    let (client, connection) = tokio_postgres::connect(connection_string, tokio_postgres::NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    Ok(client)
}
