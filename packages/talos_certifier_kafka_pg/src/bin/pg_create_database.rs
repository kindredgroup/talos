use talos_certifier_adapters::{postgres::create_database, PgConfig};

#[tokio::main]
async fn main() -> Result<(), tokio_postgres::Error> {
    let pg_config = PgConfig::from_env();
    create_database(pg_config).await
}
