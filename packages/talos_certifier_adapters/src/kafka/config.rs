use rdkafka::ClientConfig;

pub struct Config {
    pub brokers: Vec<String>,
    pub topic_prefix: String,
    pub consumer_topic: String,
    pub producer_topic: String,
    pub client_id: String,
    pub group_id: String,
    pub username: String,
    pub password: String,
}

impl Config {
    pub fn build_consumer_config(&self) -> ClientConfig {
        let mut client_config = ClientConfig::new();

        let username = self.username.to_owned();
        let password = self.password.to_owned();

        client_config
            .set("group.id", &self.group_id)
            .set("bootstrap.servers", self.brokers.join(","))
            .set("auto.offset.reset", "earliest")
            .set("socket.keepalive.enable", "true")
            // .set("auto.commit.interval.ms", "5000")
            // .set("enable.auto.offset.store", "false")
            .set("enable.auto.commit", "false");
        // .set("fetch.min.bytes", "524288")
        // .set("fetch.wait.max.ms", "500")
        // .set("max.partition.fetch.bytes", "1048576");

        if !username.is_empty() && !password.is_empty() {
            client_config
                .set("security.protocol", "SASL_PLAINTEXT")
                .set("sasl.mechanisms", "SCRAM-SHA-512")
                .set("sasl.username", username)
                .set("sasl.password", password);
        }

        client_config
    }

    pub fn build_producer_config(&self) -> ClientConfig {
        let mut client_config = ClientConfig::new();

        let username = self.username.to_owned();
        let password = self.password.to_owned();

        client_config
            .set("bootstrap.servers", self.brokers.join(","))
            .set("message.timeout.ms", "30000");

        if !username.is_empty() && !password.is_empty() {
            client_config
                .set("security.protocol", "SASL_PLAINTEXT")
                .set("sasl.mechanisms", "SCRAM-SHA-512")
                .set("sasl.username", username)
                .set("sasl.password", password);
        }

        client_config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_test_kafka_config() -> Config {
        Config {
            brokers: vec!["broker1".to_string()],
            topic_prefix: "".to_owned(),
            consumer_topic: "consumer-topic-1".to_owned(),
            producer_topic: "producer-topic-1".to_owned(),
            client_id: "client-id-1".to_string(),
            group_id: "group-id-1".to_string(),
            username: "".to_owned(),
            password: "".to_owned(),
        }
    }
    #[test]
    fn test_build_consumer_config_obj() {
        let config = build_test_kafka_config().build_consumer_config();
        assert_eq!(config.get("group.id").unwrap(), "group-id-1");
        assert_eq!(config.get("socket.keepalive.enable").unwrap(), "true");
        assert!(config.get("sasl.username").is_none());
    }
    #[test]
    fn test_passing_credentials_to_build_consumer_config() {
        let config = Config {
            brokers: vec!["broker1".to_string()],
            topic_prefix: "".to_owned(),
            consumer_topic: "consumer-topic-1".to_owned(),
            producer_topic: "producer-topic-1".to_owned(),
            client_id: "client-id-1".to_string(),
            group_id: "groud-id-1".to_string(),
            username: "user".to_string(),
            password: "password".to_string(),
        };
        let config = config.build_consumer_config();
        assert_eq!(config.get("sasl.username").unwrap(), "user");
        assert_eq!(config.get("sasl.password").unwrap(), "password");
        assert_eq!(config.get("security.protocol").unwrap(), "SASL_PLAINTEXT");
    }

    #[test]
    fn test_build_producer_config_obj() {
        let config = build_test_kafka_config().build_producer_config();
        assert!(config.get("group.id").is_none());
        assert!(config.get("sasl.username").is_none());
    }
    #[test]
    fn test_passing_credentials_to_build_producer_config() {
        let config = Config {
            brokers: vec!["broker1".to_string()],
            topic_prefix: "".to_owned(),
            consumer_topic: "consumer-topic-1".to_owned(),
            producer_topic: "producer-topic-1".to_owned(),
            client_id: "client-id-1".to_string(),
            group_id: "groud-id-1".to_string(),
            username: "user".to_string(),
            password: "password".to_string(),
        };
        let config = config.build_producer_config();
        assert_eq!(config.get("sasl.username").unwrap(), "user");
        assert_eq!(config.get("sasl.password").unwrap(), "password");
        assert_eq!(config.get("security.protocol").unwrap(), "SASL_PLAINTEXT");
    }
}
