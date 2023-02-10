use talos_certifier_adapters::{postgres::create_database, PgConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let pg_config = PgConfig::from_env();
    Ok(create_database(pg_config).await?)
}
