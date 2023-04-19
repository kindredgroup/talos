#[derive(Clone, Debug)]
pub struct DatabaseConfig {
    pub user: String,
    pub password: String,
    pub host: String,
    pub port: String,
    pub database: String,
}

impl DatabaseConfig {
    pub fn get_connection_string(&self, database: &str) -> String {
        format!("postgres://{}:{}@{}:{}/{}", self.user, self.password, self.host, self.port, database)
    }

    pub fn get_public_connection_string(&self, database: &str) -> String {
        format!("postgres://{}:***@{}:{}/{}", self.user, self.host, self.port, database)
    }
}
