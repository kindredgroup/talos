use refinery::Runner;
use tokio_postgres::{error::SqlState, tls::NoTlsStream, Client, Connection, Socket};

use crate::PgConfig;

/// Connect to postgres using the connection string provided
async fn pg_connect(connection_string: String) -> Result<(Client, Connection<Socket, NoTlsStream>), tokio_postgres::Error> {
    let connection = tokio_postgres::connect(connection_string.as_str(), tokio_postgres::NoTls).await?;
    Ok(connection)
}

/// Create database
async fn create_db(client: &Client, database: &str) -> Result<(), tokio_postgres::Error> {
    let create_db_ddl = format!("CREATE DATABASE \"{}\" ", database);
    client.execute(&create_db_ddl, &[]).await?;

    Ok(())
}

pub async fn create_database(pg_config: PgConfig) -> Result<(), tokio_postgres::Error> {
    println!("Establishing connection to postgres server");
    if let Err(error) = pg_connect(pg_config.get_database_connection_string()).await {
        //Create database if connection to database failed as the database doesn't exist.
        if let Some(&SqlState::UNDEFINED_DATABASE) = error.code() {
            println!("Database {} not found, creating one!!!", pg_config.database);

            let (client, connection) = pg_connect(pg_config.get_base_connection_string()).await?;

            // Connect to db
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("connection error: {}", e);
                }
            });

            create_db(&client, &pg_config.database).await?;
        } else {
            eprintln!("Error... {:#?}", error.code());
            return Err(error);
        }
    };

    println!("Completed all operations, exiting app");

    Ok(())
}

pub async fn run_migration(pg_config: PgConfig, runner: Runner) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    println!("Running DB migrations...");
    let (mut client, con) = tokio_postgres::connect(pg_config.get_database_connection_string().as_str(), tokio_postgres::NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = con.await {
            eprintln!("connection error: {}", e);
        }
    });

    let migration_report = runner.run_async(&mut client).await?;
    for migration in migration_report.applied_migrations() {
        println!("Migration Applied -  Name: {}, Version: {}", migration.name(), migration.version());
    }
    println!("DB migrations finished!");

    Ok(())
}
