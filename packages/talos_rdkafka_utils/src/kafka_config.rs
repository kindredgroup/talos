use std::{collections::HashMap, env};

use rdkafka::{config::RDKafkaLogLevel, ClientConfig};
use talos_common_utils::{env_var, env_var_with_defaults};

#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub brokers: Vec<String>,
    pub topic: String,
    pub client_id: String,
    pub group_id: String,
    pub username: String,
    pub password: String,
    pub producer_config_overrides: HashMap<String, String>,
    pub consumer_config_overrides: HashMap<String, String>,
    // Controls how long to wait until message is successfully placed on the librdkafka producer queue  (including retries).
    pub producer_send_timeout_ms: Option<u32>,
    pub log_level: Option<String>,
}

impl KafkaConfig {
    pub fn from_env(prefix: Option<&'static str>) -> Self {
        let prefix = if let Some(prefix) = prefix { format!("{}_", prefix) } else { "".to_string() };

        let log_level = env_var_with_defaults!(format!("{}KAFKA_LOG_LEVEL", prefix), String, "info".to_string());
        let mut cfg = KafkaConfig {
            brokers: env_var!(format!("{}KAFKA_BROKERS", prefix), Vec<String>),
            topic: env_var!(format!("{}KAFKA_TOPIC", prefix)),
            client_id: env_var!(format!("{}KAFKA_CLIENT_ID", prefix)),
            group_id: env_var!(format!("{}KAFKA_GROUP_ID", prefix)),
            username: env_var!(format!("{}KAFKA_USERNAME", prefix)),
            password: env_var!(format!("{}KAFKA_PASSWORD", prefix)),
            producer_config_overrides: HashMap::new(),
            consumer_config_overrides: HashMap::new(),
            producer_send_timeout_ms: None,
            log_level: if log_level.is_empty() { None } else { Some(log_level) },
        };

        let consumer_prefix = format!("{}KAFKA_CONSUMER_OVERRIDES.", prefix);
        for (name, _) in env::vars().filter(|(k, _)| k.starts_with(consumer_prefix.as_str())) {
            let property = name.replace(consumer_prefix.as_str(), "");
            cfg.consumer_config_overrides.insert(property.clone(), env_var!(property.clone()));
        }

        let producer_prefix = format!("{}KAFKA_PRODUCER_OVERRIDES.", prefix);
        for (name, _) in env::vars().filter(|(name, _)| name.starts_with(producer_prefix.as_str())) {
            let property = name.replace(producer_prefix.as_str(), "");
            cfg.producer_config_overrides.insert(property.clone(), env_var!(property.clone()));
        }

        cfg
    }

    pub fn extend(&mut self, producer_config_overrides: Option<HashMap<String, String>>, consumer_config_overrides: Option<HashMap<String, String>>) {
        if let Some(more_values) = producer_config_overrides {
            self.producer_config_overrides.extend(more_values);
        }
        if let Some(more_values) = consumer_config_overrides {
            self.consumer_config_overrides.extend(more_values);
        }
    }

    pub fn set_overrides(&mut self, producer_config_overrides: HashMap<String, String>, consumer_config_overrides: HashMap<String, String>) {
        self.producer_config_overrides = producer_config_overrides;
        self.consumer_config_overrides = consumer_config_overrides;
    }

    pub fn build_consumer_config(&self) -> ClientConfig {
        let mut client_config = ClientConfig::new();

        let brokers = self.brokers.join(",");
        let base_config = HashMap::<String, String>::from([
            ("group.id".to_string(), self.group_id.clone()),
            ("bootstrap.servers".to_string(), brokers),
            ("socket.keepalive.enable".to_string(), "true".to_string()),
        ]);

        for (k, v) in base_config.iter() {
            client_config.set(k, v);
        }
        for (k, v) in self.consumer_config_overrides.iter() {
            client_config.set(k, v);
        }

        self.setup_auth(&mut client_config, base_config);
        if let Some(ref level) = self.log_level {
            client_config.set_log_level(Self::map_log_level((*level).clone()));
        }

        client_config
    }

    pub fn build_producer_config(&self) -> ClientConfig {
        let mut client_config = ClientConfig::new();

        let brokers = self.brokers.join(",");
        let base_config = HashMap::<String, String>::from([
            ("message.timeout.ms".to_string(), "30000".to_string()),
            ("bootstrap.servers".to_string(), brokers),
            ("message.send.max.retries".to_string(), "100000".to_string()),
        ]);

        for (k, v) in base_config.iter() {
            client_config.set(k, v);
        }
        for (k, v) in self.producer_config_overrides.iter() {
            client_config.set(k, v);
        }

        self.setup_auth(&mut client_config, base_config);

        log::warn!("p: client_config = {:?}", client_config);

        client_config
    }

    pub fn map_log_level(level: String) -> RDKafkaLogLevel {
        match level.to_lowercase().as_str() {
            "alert" => RDKafkaLogLevel::Alert,
            "critical" => RDKafkaLogLevel::Critical,
            "debug" => RDKafkaLogLevel::Debug,
            "emerg" => RDKafkaLogLevel::Emerg,
            "error" => RDKafkaLogLevel::Error,
            "info" => RDKafkaLogLevel::Info,
            "notice" => RDKafkaLogLevel::Notice,
            "warning" => RDKafkaLogLevel::Warning,
            _ => RDKafkaLogLevel::Info,
        }
    }

    fn setup_auth(&self, client: &mut ClientConfig, base_config: HashMap<String, String>) {
        if !self.username.is_empty() {
            client
                .set(
                    "security.protocol",
                    base_config.get("security.protocol").unwrap_or(&"SASL_PLAINTEXT".to_string()),
                )
                .set("sasl.mechanisms", base_config.get("sasl.mechanisms").unwrap_or(&"SCRAM-SHA-512".to_string()))
                .set("sasl.username", self.username.clone())
                .set("sasl.password", self.password.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::{collections::HashMap, env};

    static KAFKA_LOG_LEVEL: i32 = RDKafkaLogLevel::Error as i32;
    static KAFKA_LOG_LEVEL_VALUE: &str = "error";

    fn set_env_var(key: &str, value: &str) {
        env::set_var(key, value)
    }

    fn unset_env_var(key: &str) {
        env::remove_var(key)
    }

    fn get_kafka_env_variables(prefix: Option<&str>) -> HashMap<String, String> {
        let prefix = if let Some(prefix) = prefix { format!("{}_", prefix) } else { "".to_string() };

        let env_hashmap = [
            (format!("{}KAFKA_BROKERS", prefix), "broker1, broker2 ".to_string()),
            (format!("{}KAFKA_TOPIC", prefix), "some-topic".to_string()),
            (format!("{}KAFKA_CLIENT_ID", prefix), "some-client-id".to_string()),
            (format!("{}KAFKA_GROUP_ID", prefix), "some-group-id".to_string()),
            (format!("{}KAFKA_USERNAME", prefix), "".to_string()),
            (format!("{}KAFKA_PASSWORD", prefix), "".to_string()),
        ];
        HashMap::from(env_hashmap)
    }

    fn build_test_kafka_config() -> KafkaConfig {
        KafkaConfig {
            brokers: vec!["broker1".to_string()],
            topic: "topic".to_owned(),
            client_id: "client-id-1".to_string(),
            group_id: "group-id-1".to_string(),
            username: "user_name".to_owned(),
            password: "password".to_owned(),
            producer_config_overrides: Default::default(),
            consumer_config_overrides: Default::default(),
            producer_send_timeout_ms: None,
            log_level: Some(KAFKA_LOG_LEVEL_VALUE.into()),
        }
    }

    #[test]
    #[serial]
    fn test_from_env_gets_values_successully() {
        let prefix = None as Option<&str>;
        get_kafka_env_variables(prefix).iter().for_each(|(k, v)| {
            set_env_var(k, v);
        });

        let config = KafkaConfig::from_env(prefix);

        assert_eq!(config.client_id, "some-client-id");
        assert_eq!(config.brokers.len(), 2);

        get_kafka_env_variables(prefix).iter().for_each(|(k, _)| {
            unset_env_var(k);
        });
    }

    #[test]
    #[serial]
    fn test_from_env_gets_values_with_prefix_successully() {
        let prefix1 = Some("TEST_PREFIX1");
        get_kafka_env_variables(prefix1).iter().for_each(|(k, v)| {
            set_env_var(k, v);
        });

        let config = KafkaConfig::from_env(prefix1);

        assert_eq!(config.client_id, "some-client-id");
        assert_eq!(config.brokers.len(), 2);

        set_env_var("TEST_PREFIX2_KAFKA_CLIENT_ID", "some-client-id2");
        set_env_var("TEST_PREFIX2_KAFKA_BROKERS", "broker2-server1, broker2-server2, broker2-server3");
        set_env_var("TEST_PREFIX2_KAFKA_TOPIC", "t2");
        set_env_var("TEST_PREFIX2_KAFKA_GROUP_ID", "g2");
        set_env_var("TEST_PREFIX2_KAFKA_USERNAME", "");
        set_env_var("TEST_PREFIX2_KAFKA_PASSWORD", "");
        let config2 = KafkaConfig::from_env(Some("TEST_PREFIX2"));

        assert_eq!(config2.client_id, "some-client-id2");
        assert_eq!(config2.brokers.len(), 3);

        get_kafka_env_variables(prefix1).iter().for_each(|(k, _)| {
            unset_env_var(k);
        });
        unset_env_var("TEST_PREFIX2_KAFKA_CLIENT_ID");
        unset_env_var("TEST_PREFIX2_KAFKA_BROKERS");
        unset_env_var("TEST_PREFIX2_KAFKA_TOPIC");
        unset_env_var("TEST_PREFIX2_KAFKA_GROUP_ID");
        unset_env_var("TEST_PREFIX2_KAFKA_USERNAME");
        unset_env_var("TEST_PREFIX2_KAFKA_PASSWORD");
    }

    #[test]
    #[serial]
    #[should_panic(expected = "KAFKA_TOPIC environment variable is not defined")]
    fn test_from_env_when_env_variable_not_found() {
        let prefix = None as Option<&str>;
        get_kafka_env_variables(prefix).iter().for_each(|(k, v)| {
            set_env_var(k, v);
        });

        unset_env_var("KAFKA_TOPIC");

        let _config = KafkaConfig::from_env(prefix);

        get_kafka_env_variables(prefix).iter().for_each(|(k, _)| {
            unset_env_var(k);
        });
    }

    #[test]
    fn test_build_consumer_config_obj() {
        let config = build_test_kafka_config().build_consumer_config();
        assert_eq!(config.get("group.id").expect("group.id"), "group-id-1");
        assert_eq!(config.get("socket.keepalive.enable").expect("socket.keepalive.enable"), "true");
        assert_eq!(config.get("sasl.username").expect("sasl.username"), "user_name");
        assert_eq!(config.log_level as i32, KAFKA_LOG_LEVEL);
    }

    #[test]
    fn test_passing_credentials_to_build_consumer_config() {
        let config = KafkaConfig {
            brokers: vec!["broker1".to_string()],
            topic: "consumer-topic-1".to_owned(),
            client_id: "client-id-1".to_string(),
            group_id: "group-id-1".to_string(),
            username: "user".to_string(),
            password: "password".to_string(),
            producer_config_overrides: Default::default(),
            consumer_config_overrides: HashMap::from([("auto.offset.reset".into(), "earliest".into()), ("enable.auto.commit".into(), "false".into())]),
            producer_send_timeout_ms: None,
            log_level: None,
        };
        let client_config = config.build_consumer_config();
        assert_eq!(client_config.get("auto.offset.reset").expect("auto.offset.reset"), "earliest");
        assert_eq!(client_config.get("socket.keepalive.enable").expect("socket.keepalive.enable"), "true");
        assert_eq!(client_config.get("enable.auto.commit").expect("enable.auto.commit"), "false");
        assert_eq!(client_config.get("sasl.username").expect("sasl.username"), "user");
        assert_eq!(client_config.get("sasl.password").expect("sasl.password"), "password");
        assert_eq!(client_config.get("security.protocol").expect("security.protocol"), "SASL_PLAINTEXT");
    }

    #[test]
    fn test_build_producer_config_obj() {
        let config = build_test_kafka_config().build_producer_config();
        assert!(config.get("group.id").is_none());
        assert_eq!(config.get("sasl.username").expect("sasl.username"), "user_name");
        assert_eq!(config.get("sasl.password").expect("sasl.password"), "password");
        assert_eq!(config.get("message.timeout.ms").expect("message.timeout.ms"), "30000");
        assert_eq!(config.get("message.send.max.retries").expect("message.send.max.retries"), "100000");
        assert_eq!(config.log_level as i32, KAFKA_LOG_LEVEL);
    }

    #[test]
    fn test_passing_overrides() {
        let kafka_config = KafkaConfig {
            brokers: vec!["broker1".to_string()],
            topic: "topic".to_owned(),
            client_id: "client-id-1".to_string(),
            group_id: "groud-id-1".to_string(),
            username: "user".to_string(),
            password: "password".to_string(),
            producer_config_overrides: HashMap::from([("message.timeout.ms".into(), "10".into())]),
            consumer_config_overrides: HashMap::from([("auto.offset.reset".into(), "latest".into())]),
            producer_send_timeout_ms: None,
            log_level: None,
        };

        let producer_config = kafka_config.build_producer_config();
        assert_eq!(producer_config.get("sasl.username").expect("sasl.username"), "user");
        assert_eq!(producer_config.get("message.timeout.ms").expect("message.timeout.ms"), "10");
        let consumer_config = kafka_config.build_consumer_config();
        assert_eq!(consumer_config.get("sasl.username").expect("sasl.username"), "user");
        assert_eq!(consumer_config.get("auto.offset.reset").expect("auto.offset.reset"), "latest");
    }

    #[test]
    fn test_setup_kafka_auth() {
        let mut cfg = ClientConfig::new();
        let kafka_config = KafkaConfig {
            brokers: vec!["brokers".to_string()],
            group_id: "group_id".to_string(),
            client_id: "client_id".to_string(),
            topic: "certification_topic".to_string(),
            log_level: Some("debug".to_string()),
            username: "user1".to_string(),
            password: "".to_string(),
            producer_config_overrides: Default::default(),
            consumer_config_overrides: Default::default(),
            producer_send_timeout_ms: None,
        };
        kafka_config.setup_auth(&mut cfg, HashMap::new());
        assert!(check_key(&mut cfg, "security.protocol", "SASL_PLAINTEXT"));
        assert!(check_key(&mut cfg, "sasl.mechanisms", "SCRAM-SHA-512"));
        assert!(check_key(&mut cfg, "sasl.username", "user1"));
        assert!(check_key(&mut cfg, "sasl.password", ""));

        cfg.set("sasl.password", "pwd".to_string());
        assert!(check_key(&mut cfg, "sasl.password", "pwd"));

        cfg.set("sasl.mechanisms", "ANONYMOUS".to_string());
        assert!(check_key(&mut cfg, "sasl.mechanisms", "ANONYMOUS"));
    }

    fn check_key(cfg: &mut ClientConfig, key: &str, value: &str) -> bool {
        if let Some(v) = cfg.get(key) {
            v == value
        } else {
            false
        }
    }
}
