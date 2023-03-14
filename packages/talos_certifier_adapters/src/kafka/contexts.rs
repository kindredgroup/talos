use log::{error, info};
use rdkafka::{
    consumer::ConsumerContext,
    producer::{DeliveryResult, ProducerContext},
    ClientContext,
};
use talos_certifier::{
    core::SystemMonitorMessage,
    errors::{AdapterFailureError, SystemErrorType},
};
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct TalosKafkaClientContext {
    pub channel: mpsc::Sender<SystemMonitorMessage>,
}

impl ClientContext for TalosKafkaClientContext {
    fn error(&self, error: rdkafka::error::KafkaError, reason: &str) {
        if let Some(code) = error.rdkafka_error_code() {
            let failure_errors = vec![
                rdkafka::error::RDKafkaErrorCode::BrokerTransportFailure,
                rdkafka::error::RDKafkaErrorCode::AllBrokersDown,
                rdkafka::error::RDKafkaErrorCode::UnknownTopic,
            ];
            if failure_errors.contains(&code) {
                tokio::spawn({
                    let channel = self.channel.clone();
                    let error_clone = error.clone();
                    async move {
                        let res = channel
                            .send(SystemMonitorMessage::Failures(SystemErrorType::AdapterFailure(AdapterFailureError {
                                adapter_name: "Kafka".to_string(),
                                reason: error_clone.to_string(),
                            })))
                            .await;
                        info!(" Send channel result is {res:?}");
                    }
                });
                // panic!("librdkafka: [Gethyl Kurian] {}: {}", error, reason);
            }
        }
        error!("librdkafka: {}: {}", error, reason);
    }
}

// Consumer Context
pub type TalosKafkaConsumerContext = TalosKafkaClientContext;

impl ConsumerContext for TalosKafkaConsumerContext {}

// Producer Context
pub type TalosKafkaProducerContext = TalosKafkaClientContext;

impl ProducerContext for TalosKafkaProducerContext {
    type DeliveryOpaque = ();

    fn delivery(&self, _: &DeliveryResult<'_>, _: Self::DeliveryOpaque) {}
}
