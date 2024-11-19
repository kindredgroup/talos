use log::warn;
use talos_common_utils::{env_var, env_var_with_defaults};

#[derive(Debug, Clone)]
pub struct PgConfig {
    pub user: String,
    pub password: String,
    pub host: String,
    pub port: String,
    pub database: String,
    pub pool_size: Option<u32>,
}

impl PgConfig {
    pub fn from_env() -> PgConfig {
        let pool_size = env_var_with_defaults!("PG_POOL_SIZE", Option::<u32>);
        warn!("Pool size used... {pool_size:?}");
        PgConfig {
            user: env_var!("PG_USER"),
            password: env_var!("PG_PASSWORD"),
            host: env_var!("PG_HOST"),
            port: env_var!("PG_PORT"),
            database: env_var!("PG_DATABASE"),
            pool_size,
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

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, env};

    use serial_test::serial;

    use super::*;

    fn set_env_var(key: &str, value: &str) {
        env::set_var(key, value)
    }

    fn unset_env_var(key: &str) {
        env::remove_var(key)
    }

    fn get_pg_env_variables() -> HashMap<&'static str, &'static str> {
        let env_hashmap = [
            ("PG_USER", "test-user"),
            ("PG_PASSWORD", "test-pwd"),
            ("PG_HOST", "some-host"),
            ("PG_PORT", "5432"),
            ("PG_DATABASE", "test-db"),
        ];
        HashMap::from(env_hashmap)
    }

    #[test]
    #[serial]
    fn test_from_env_gets_values_successully() {
        get_pg_env_variables().iter().for_each(|(k, v)| {
            set_env_var(k, v);
        });

        let config = PgConfig::from_env();

        assert_eq!(config.database, "test-db");
        assert_eq!(config.port, "5432");
        assert_eq!(config.user, "test-user");
        assert_eq!(config.password, "test-pwd");
        assert_eq!(config.host, "some-host");

        get_pg_env_variables().iter().for_each(|(k, _)| {
            unset_env_var(k);
        });
    }
    #[test]
    #[serial]
    #[should_panic(expected = "PG_DATABASE environment variable is not defined")]
    fn test_from_env_when_env_variable_not_found() {
        get_pg_env_variables().iter().for_each(|(k, v)| {
            set_env_var(k, v);
        });

        unset_env_var("PG_DATABASE");

        let _config = PgConfig::from_env();

        get_pg_env_variables().iter().for_each(|(k, _)| {
            unset_env_var(k);
        });
    }

    #[test]
    #[serial]
    fn test_pg_connection_string() {
        get_pg_env_variables().iter().for_each(|(k, v)| {
            set_env_var(k, v);
        });

        let config = PgConfig::from_env();

        assert_eq!(
            config.get_base_connection_string(),
            format!("postgres://{}:{}@{}:{}", config.user, config.password, config.host, config.port)
        );

        assert_eq!(
            config.get_database_connection_string(),
            format!(
                "postgres://{}:{}@{}:{}/{}",
                config.user, config.password, config.host, config.port, config.database
            )
        );

        get_pg_env_variables().iter().for_each(|(k, _)| {
            unset_env_var(k);
        });
    }
}
