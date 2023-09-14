use talos_common_utils::{env_var, env_var_with_defaults};

#[derive(Clone, Debug)]
pub struct DatabaseConfig {
    pub pool_size: u32,
    pub user: String,
    pub password: String,
    pub host: String,
    pub port: String,
    pub database: String,
}

impl DatabaseConfig {
    pub fn from_env(prefix: Option<&'static str>) -> Result<Self, String> {
        let prefix = if let Some(prefix) = prefix { format!("{}_", prefix) } else { "".into() };
        Ok(Self {
            pool_size: env_var_with_defaults!(format!("{}PG_POOL_SIZE", prefix), u32, 10),
            host: env_var_with_defaults!(format!("{}PG_HOST", prefix), String, "127.0.0.1".into()),
            port: env_var!(format!("{}PG_PORT", prefix)),
            user: env_var!(format!("{}PG_USER", prefix)),
            password: env_var!(format!("{}PG_PASSWORD", prefix)),
            database: env_var!(format!("{}PG_DATABASE", prefix)),
        })
    }

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
            pool_size: 1,
            user: "USER1".into(),
            password: "1234".into(),
            host: "localhost".into(),
            port: "1010".into(),
            database: "db_admin".into(),
        };
        assert_eq!(
            format!("{:?}", cfg.clone()),
            r#"DatabaseConfig { pool_size: 1, user: "USER1", password: "1234", host: "localhost", port: "1010", database: "db_admin" }"#,
        );
    }

    #[test]
    fn should_generate_valid_connection_string() {
        let cfg = DatabaseConfig {
            pool_size: 1,
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
