use talos_certifier_adapters::{postgres::run_migration, PgConfig};

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("migrations");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let pg_config = PgConfig::from_env();

    run_migration(pg_config, embedded::migrations::runner()).await
}
