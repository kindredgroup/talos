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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::redundant_clone)]
    fn test_model() {
        let cfg = DatabaseConfig {
            user: "USER1".into(),
            password: "1234".into(),
            host: "localhost".into(),
            port: "1010".into(),
            database: "db_admin".into(),
        };
        assert_eq!(
            format!("{:?}", cfg.clone()),
            r#"DatabaseConfig { user: "USER1", password: "1234", host: "localhost", port: "1010", database: "db_admin" }"#,
        );
    }

    #[test]
    fn should_generate_valid_connection_string() {
        let cfg = DatabaseConfig {
            user: "USER1".into(),
            password: "1234".into(),
            host: "localhost".into(),
            port: "1010".into(),
            database: "db_admin".into(),
        };

        assert_eq!(cfg.get_connection_string("db_app").as_str(), "postgres://USER1:1234@localhost:1010/db_app");
        assert_eq!(
            cfg.get_public_connection_string("db_app").as_str(),
            "postgres://USER1:***@localhost:1010/db_app",
        );
    }
}
