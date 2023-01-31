#[derive(Debug, Clone)]
pub struct PgConfig {
    pub user: String,
    pub password: String,
    pub host: String,
    pub port: String,
    pub database: String,
}

impl PgConfig {
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
