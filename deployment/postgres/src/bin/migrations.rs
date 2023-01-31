use postgres::config::PgConfig;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("migrations");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let pg_config = PgConfig::new();

    println!("Running DB migrations...");
    let (mut client, con) = tokio_postgres::connect(pg_config.get_database_connection_string().as_str(), tokio_postgres::NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = con.await {
            eprintln!("connection error: {}", e);
        }
    });

    let migration_report = embedded::migrations::runner().run_async(&mut client).await?;
    for migration in migration_report.applied_migrations() {
        println!("Migration Applied -  Name: {}, Version: {}", migration.name(), migration.version());
    }
    println!("DB migrations finished!");

    Ok(())
}
