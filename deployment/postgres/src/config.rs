use std::env;

pub struct PgConfig {
    pub user: String,
    pub password: String,
    pub host: String,
    pub port: String,
    pub database: String,
}

impl PgConfig {
    pub fn new() -> Self {
        PgConfig {
            user: env::var("PG_USER").expect("$PG_USER not set"),
            password: env::var("PG_PASSWORD").expect("$PG_PASSWORD not set"),
            host: env::var("PG_HOST").expect("$PG_HOST not set"),
            port: env::var("PG_PORT").expect("$PG_PORT not set"),
            database: env::var("PG_DATABASE").expect("$PG_DATABASE not set"),
        }
    }

    pub fn get_base_connection_string(&self) -> String {
        let PgConfig {
            user, password, host, port, ..
        } = self;
        format!("postgres://{user}:{password}@{host}:{port}")
    }

    pub fn get_database_connection_string(&self) -> String {
        let PgConfig { database, .. } = self;
        let base_connection_string = self.get_base_connection_string();
        format!("{base_connection_string}/{database}")
    }
}
impl Default for PgConfig {
    fn default() -> Self {
        Self::new()
    }
}
