use log::debug;
use serde::{Deserialize, Serialize};
use std::env;
use std::fmt::Debug;
use std::str::FromStr;

trait EnvVarFromStr {
    fn from_env_var_str(name: &str, v: &str) -> Self;
}

macro_rules! from_str_env_var_types {
  ($type: ty) => {
    impl EnvVarFromStr for $type {
      fn from_env_var_str(name: &str, v: &str) -> Self {
        Self::from_str(&v)
          .map_err(|e| format!("{} environment variable failed to parse with error {}", name, e))
          .unwrap()
      }
    }
  };

  ($($type: ty),+) => {
    $( from_str_env_var_types!( $type ); )+
  };
}

from_str_env_var_types!(String, u32);

impl<T: EnvVarFromStr> EnvVarFromStr for Vec<T> {
    fn from_env_var_str(name: &str, v: &str) -> Self {
        let x = v.split(',').map(|v| T::from_env_var_str(name, v));
        x.collect()
    }
}

fn var<T: EnvVarFromStr>(name: &str) -> T {
    let v = env::var(name).unwrap_or_else(|_| panic!("{} environment variable is not defined", name));
    T::from_env_var_str(name, &v)
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Config {
    // Kafka
    pub kafka_brokers: Vec<String>,
    pub kafka_topic: String,
    pub kafka_client_id: String,
    pub kafka_group_id: String,
    pub kafka_username: String,
    pub kafka_password: String,
    // Postgres
    pub pg_host: String,
    pub pg_port: u32,
    pub pg_user: String,
    pub pg_password: String,
    pub pg_database: String,
}

impl Config {
    pub fn from_env() -> Self {
        debug!("Loading config from environment variables");

        let config = Self {
            kafka_brokers: var("KAFKA_BROKERS"),
            kafka_topic: var("KAFKA_TOPIC"),
            kafka_client_id: var("KAFKA_CLIENT_ID"),
            kafka_group_id: var("KAFKA_GROUP_ID"),
            kafka_username: var("KAFKA_USERNAME"),
            kafka_password: var("KAFKA_PASSWORD"),
            pg_host: var("PG_HOST"),
            pg_port: var("PG_PORT"),
            pg_user: var("PG_USER"),
            pg_password: var("PG_PASSWORD"),
            pg_database: var("PG_DATABASE"),
        };

        log::debug!("Config loaded from environment variables: {:?}", config);

        config
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            // Kafka
            kafka_brokers: vec!["".to_owned()],
            kafka_topic: "".to_owned(),
            kafka_client_id: "".to_owned(),
            kafka_group_id: "".to_owned(),
            kafka_username: "".to_owned(),
            kafka_password: "".to_owned(),
            // Postgres
            pg_host: "".to_owned(),
            pg_port: 1234,
            pg_user: "".to_owned(),
            pg_password: "".to_owned(),
            pg_database: "".to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn read_env_to_config() {
        env::set_var("ENVIRONMENT", "dev");
        env::set_var("SERVICE_NAME", "serv");
        env::set_var("BUILD_VERSION", "0.0.1");
        env::set_var("KAFKA_BROKERS", "localhost:9093,foo:9094");
        env::set_var("KAFKA_TOPIC", "consumer.topic");
        env::set_var("KAFKA_CLIENT_ID", "client");
        env::set_var("KAFKA_GROUP_ID", "group");
        env::set_var("KAFKA_USERNAME", "kfadmin");
        env::set_var("KAFKA_PASSWORD", "kfpass");
        env::set_var("PG_HOST", "localhost");
        env::set_var("PG_PORT", "5432");
        env::set_var("PG_USER", "pgadmin");
        env::set_var("PG_PASSWORD", "pgpass");
        env::set_var("PG_DATABASE", "db");

        // Initialize config from environment variables
        let config = Config::from_env();
        assert_eq!(
            config,
            Config {
                kafka_brokers: vec!["localhost:9093".to_owned(), "foo:9094".to_owned()],
                kafka_topic: "consumer.topic".to_owned(),
                kafka_client_id: "client".to_owned(),
                kafka_group_id: "group".to_owned(),
                kafka_username: "kfadmin".to_owned(),
                kafka_password: "kfpass".to_owned(),
                pg_host: "localhost".to_owned(),
                pg_port: 5432,
                pg_user: "pgadmin".to_owned(),
                pg_password: "pgpass".to_owned(),
                pg_database: "db".to_owned(),
            }
        );
    }
}
