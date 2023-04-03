use std::env::{var, VarError};
use std::num::ParseIntError;

use rdkafka::config::RDKafkaLogLevel;

use talos_agent::api::{AgentConfig, KafkaConfig, TalosType};

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

    pub fn load() -> Result<(AgentConfig, KafkaConfig), String> {
        let cfg_agent = AgentConfig {
            agent: Self::read_var("AGENT_NAME").unwrap(),
            cohort: Self::read_var("COHORT_NAME").unwrap(),
            buffer_size: Self::read_var("AGENT_BUFFER_SIZE").unwrap().parse().unwrap(),
            timout_ms: Self::read_var("AGENT_TIMEOUT_MS").unwrap().parse().unwrap(),
        };

        let cfg_kafka = KafkaConfig {
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
        };

        log::info!("Configs:\n\t{:?}\n\t{:?}", cfg_agent, cfg_kafka);
        Ok((cfg_agent, cfg_kafka))
    }
}
