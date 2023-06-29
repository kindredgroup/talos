use std::env::{var, VarError};
use std::num::ParseIntError;
use std::string::ToString;

use rdkafka::config::RDKafkaLogLevel;

use talos_agent::api::{AgentConfig, KafkaConfig, TalosType};

use crate::state::postgres::database_config::DatabaseConfig;

pub struct ConfigLoader {}

impl ConfigLoader {
    fn get_kafka_log_level_from_env() -> Result<RDKafkaLogLevel, String> {
        match Self::read_var("KAFKA_LOG_LEVEL") {
            Ok(level) => match level.to_lowercase().as_str() {
                "alert" => Ok(RDKafkaLogLevel::Alert),
                "critical" => Ok(RDKafkaLogLevel::Critical),
                "debug" => Ok(RDKafkaLogLevel::Debug),
                "emerg" => Ok(RDKafkaLogLevel::Emerg),
                "error" => Ok(RDKafkaLogLevel::Error),
                "info" => Ok(RDKafkaLogLevel::Info),
                "notice" => Ok(RDKafkaLogLevel::Notice),
                "warning" => Ok(RDKafkaLogLevel::Warning),
                _ => Ok(RDKafkaLogLevel::Info),
            },

            Err(e) => Err(e),
        }
    }

    fn read_var_optional(name: &str) -> Result<Option<String>, String> {
        match var(name) {
            Ok(value) => {
                if value.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(value.trim().to_string()))
                }
            }
            Err(e) => match e {
                VarError::NotPresent => {
                    log::info!("Environment variable is not found: \"{}\"", name);
                    Ok(None)
                }
                VarError::NotUnicode(_) => Err(format!("Environment variable is not unique: \"{}\"", name)),
            },
        }
    }

    fn read_var(name: &str) -> Result<String, String> {
        match var(name) {
            Ok(value) => {
                if value.is_empty() {
                    Err(format!("Environment variable is not set: \"{}\"", name))
                } else {
                    Ok(value.trim().to_string())
                }
            }
            Err(e) => match e {
                VarError::NotPresent => Err(format!("Environment variable is not found: \"{}\"", name)),
                VarError::NotUnicode(_) => Err(format!("Environment variable is not unique: \"{}\"", name)),
            },
        }
    }

    pub fn load() -> Result<(AgentConfig, KafkaConfig, DatabaseConfig), String> {
        let cfg_agent = Self::load_agent_config()?;
        let cfg_kafka = Self::load_kafka_config()?;
        let cfg_db = Self::load_db_config()?;
        log::info!("Configs:\n\t{:?}\n\t{:?}\n\t{:?}", cfg_agent, cfg_kafka, cfg_db);
        Ok((cfg_agent, cfg_kafka, cfg_db))
    }

    pub fn load_db_config() -> Result<DatabaseConfig, String> {
        Ok(DatabaseConfig {
            user: Self::read_var("COHORT_PG_USER")?,
            password: Self::read_var("COHORT_PG_PASSWORD")?,
            host: Self::read_var("COHORT_PG_HOST")?,
            port: Self::read_var("COHORT_PG_PORT")?,
            database: Self::read_var("COHORT_PG_DATABASE")?,
        })
    }

    pub fn load_kafka_config() -> Result<KafkaConfig, String> {
        Ok(KafkaConfig {
            brokers: Self::read_var("KAFKA_BROKERS")?,
            group_id: Self::read_var("KAFKA_GROUP_ID")?,
            certification_topic: Self::read_var("KAFKA_TOPIC")?,
            fetch_wait_max_ms: Self::read_var("KAFKA_FETCH_WAIT_MAX_MS")?.parse().map_err(|e: ParseIntError| e.to_string())?,
            message_timeout_ms: Self::read_var("KAFKA_MESSAGE_TIMEOUT_MS")?.parse().map_err(|e: ParseIntError| e.to_string())?,
            enqueue_timeout_ms: Self::read_var("KAFKA_ENQUEUE_TIMEOUT_MS")?.parse().map_err(|e: ParseIntError| e.to_string())?,
            log_level: Self::get_kafka_log_level_from_env()?,
            talos_type: TalosType::External,
            sasl_mechanisms: Self::read_var_optional("KAFKA_SASL_MECHANISMS")?,
            username: Self::read_var_optional("KAFKA_USERNAME")?,
            password: Self::read_var_optional("KAFKA_PASSWORD")?,
        })
    }

    pub fn load_agent_config() -> Result<AgentConfig, String> {
        Ok(AgentConfig {
            agent: Self::read_var("AGENT_NAME").unwrap(),
            cohort: Self::read_var("COHORT_NAME").unwrap(),
            buffer_size: Self::read_var("AGENT_BUFFER_SIZE").unwrap().parse().unwrap(),
            timout_ms: Self::read_var("AGENT_TIMEOUT_MS").unwrap().parse().unwrap(),
        })
    }
}

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use std::env;
    use std::sync::Mutex;

    use crate::config_loader::ConfigLoader;

    use super::*;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn read_var() {
        env::set_var("__UNIT_TEST_RANDOM-1", "12345");
        let result = ConfigLoader::read_var("__UNIT_TEST_RANDOM-1");
        assert_eq!(result.unwrap(), "12345");
    }

    #[test]
    fn read_var_should_fail_when_missing() {
        let result = ConfigLoader::read_var("__UNIT_TEST_RANDOM-2");
        assert!(result.is_err());
        if let Err(ref e) = result {
            assert_eq!(*e, "Environment variable is not found: \"__UNIT_TEST_RANDOM-2\"".to_string());
        }
        env::set_var("__UNIT_TEST_RANDOM-3", "");
        let result = ConfigLoader::read_var("__UNIT_TEST_RANDOM-3");
        assert!(result.is_err());
        if let Err(ref e) = result {
            assert_eq!(*e, "Environment variable is not set: \"__UNIT_TEST_RANDOM-3\"".to_string());
        }
    }

    #[test]
    fn read_optional_should_not_fail() {
        let result = ConfigLoader::read_var_optional("__UNIT_TEST_RANDOM-4");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);

        env::set_var("__UNIT_TEST_RANDOM-5", "12345");
        let result = ConfigLoader::read_var_optional("__UNIT_TEST_RANDOM-5");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().unwrap(), "12345".to_string());

        env::set_var("__UNIT_TEST_RANDOM-6", "");
        let result = ConfigLoader::read_var_optional("__UNIT_TEST_RANDOM-6");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn get_kafka_log_level_from_env() {
        let _unused = ENV_LOCK.lock().unwrap();

        let test_data: Vec<(&str, i32)> = vec![
            ("alert", 1),
            ("critical", 2),
            ("debug", 7),
            ("emerg", 0),
            ("error", 3),
            ("info", 6),
            ("notice", 5),
            ("warning", 4),
            ("unknown", 6),
        ];

        for (level, code) in test_data.iter() {
            env::set_var("KAFKA_LOG_LEVEL", *level);
            let result = ConfigLoader::get_kafka_log_level_from_env();
            assert!(result.is_ok());
            assert_eq!(result.unwrap() as i32, *code);
        }
    }

    #[test]
    fn get_kafka_log_level_from_env_should_fail_if_not_set() {
        let _unused = ENV_LOCK.lock().unwrap();
        env::set_var("KAFKA_LOG_LEVEL", "");
        let result = ConfigLoader::get_kafka_log_level_from_env();
        assert!(result.is_err());
    }

    #[test]
    fn load() {
        let _unused = ENV_LOCK.lock().unwrap();

        env::set_var("AGENT_NAME", "aName");
        env::set_var("COHORT_NAME", "cName");
        env::set_var("AGENT_BUFFER_SIZE", "1");
        env::set_var("AGENT_TIMEOUT_MS", "2");

        env::set_var("KAFKA_LOG_LEVEL", "info");
        env::set_var("KAFKA_BROKERS", "kBrokers");
        env::set_var("KAFKA_GROUP_ID", "kGroup");
        env::set_var("KAFKA_TOPIC", "kTopic");
        env::set_var("KAFKA_FETCH_WAIT_MAX_MS", "3");
        env::set_var("KAFKA_MESSAGE_TIMEOUT_MS", "4");
        env::set_var("KAFKA_ENQUEUE_TIMEOUT_MS", "5");
        env::set_var("KAFKA_SASL_MECHANISMS", "kSasl");
        env::set_var("KAFKA_USERNAME", "kUser");
        env::set_var("KAFKA_PASSWORD", "kPwd");

        env::set_var("COHORT_PG_USER", "testPgUser");
        env::set_var("COHORT_PG_PASSWORD", "***");
        env::set_var("COHORT_PG_HOST", "some host");
        env::set_var("COHORT_PG_PORT", "0");
        env::set_var("COHORT_PG_DATABASE", "not-existing");

        let result = ConfigLoader::load();
        let (a, k, _) = result.map_err(|e| assert_eq!("no error is expected", e)).unwrap();

        assert_eq!(a.agent, "aName".to_string());
        assert_eq!(a.cohort, "cName".to_string());
        assert_eq!(a.timout_ms, 2_u64);
        assert_eq!(a.buffer_size, 1_usize);

        assert_eq!(k.brokers, "kBrokers".to_string());
        assert_eq!(k.group_id, "kGroup".to_string());
        assert_eq!(k.certification_topic, "kTopic".to_string());
        assert_eq!(k.fetch_wait_max_ms, 3_u64);
        assert_eq!(k.message_timeout_ms, 4_u64);
        assert_eq!(k.enqueue_timeout_ms, 5_u64);
        assert_eq!(k.sasl_mechanisms, Some("kSasl".to_string()));
        assert_eq!(k.username, Some("kUser".to_string()));
        assert_eq!(k.password, Some("kPwd".to_string()));

        env::remove_var("KAFKA_SASL_MECHANISMS");
        env::remove_var("KAFKA_USERNAME");
        env::remove_var("KAFKA_PASSWORD");

        let result = ConfigLoader::load();
        assert!(result.is_ok());
        let (_, k, _) = result.unwrap();
        assert_eq!(k.sasl_mechanisms, None);
        assert_eq!(k.username, None);
        assert_eq!(k.password, None);
    }
}
// $coverage:ignore-end
