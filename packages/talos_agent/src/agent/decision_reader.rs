use opentelemetry::{global, KeyValue};
use talos_common_utils::otel::metric_constants::{
    METRIC_KEY_CERT_DECISION_TYPE, METRIC_KEY_CERT_MESSAGE_TYPE, METRIC_METER_NAME_COHORT_SDK, METRIC_NAME_AGENT_OFFSET_LAG, METRIC_NAME_CERTIFICATION_OFFSET,
    METRIC_VALUE_CERT_DECISION_TYPE_UNKNOWN, METRIC_VALUE_CERT_MESSAGE_TYPE_DECISION,
};

use crate::messaging::api::{ConsumerType, TraceableDecision};
use crate::mpsc::core::Sender;
use std::cmp::max;
use std::sync::Arc;

pub struct DecisionReaderService<S: Sender<Data = TraceableDecision>> {
    consumer: Arc<Box<ConsumerType>>,
    tx_decision: S,
}

impl<S: Sender<Data = TraceableDecision>> DecisionReaderService<S> {
    pub fn new(consumer: Arc<Box<ConsumerType>>, tx_decision: S) -> Self {
        DecisionReaderService { consumer, tx_decision }
    }

    pub async fn run(&self) {
        let mut candidate_offset: u64 = 0;
        let metric_agent_lag = global::meter(METRIC_METER_NAME_COHORT_SDK).u64_gauge(METRIC_NAME_AGENT_OFFSET_LAG).build();
        let metric_certification_offset = global::meter(METRIC_METER_NAME_COHORT_SDK).u64_gauge(METRIC_NAME_CERTIFICATION_OFFSET).build();
        loop {
            match self.read().await {
                Ok((is_decision, offset)) => {
                    if !is_decision {
                        candidate_offset = offset;
                    } else {
                        let lag = max(0, candidate_offset - offset);
                        metric_agent_lag.record(lag, &[]);
                        metric_certification_offset.record(
                            lag,
                            &[
                                KeyValue::new(METRIC_KEY_CERT_MESSAGE_TYPE, METRIC_VALUE_CERT_MESSAGE_TYPE_DECISION),
                                KeyValue::new(METRIC_KEY_CERT_DECISION_TYPE, METRIC_VALUE_CERT_DECISION_TYPE_UNKNOWN),
                            ],
                        );
                    }
                }
                Err(ReaderError::ChannelIsClosed { e: reason }) => {
                    tracing::error!("Destination channel is closed: {}", reason);
                    break;
                }
                _ => {}
            }
        }
    }

    async fn read(&self) -> Result<(bool, u64), ReaderError> {
        match self.consumer.receive_message().await {
            Ok(response) => {
                if !response.is_decision || response.decision.is_none() {
                    return Ok::<(bool, u64), ReaderError>((response.is_decision, response.offset));
                }

                let decision = response.decision.unwrap();
                self.tx_decision
                    .send(decision)
                    .await
                    .map_err(|send_error| ReaderError::ChannelIsClosed { e: send_error.to_string() })?;

                Ok((true, response.offset))
            }
            Err(err) => {
                tracing::warn!("Error receiving message: {}", err);
                Err(ReaderError::ConsumerError)
            }
        }
    }
}

enum ReaderError {
    ChannelIsClosed { e: String },
    ConsumerError,
}

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::api::TalosType;
    use crate::messaging::api::Decision::Committed;
    use crate::messaging::api::{Consumer, DecisionMessage, ReceivedMessage};
    use crate::messaging::errors::{MessagingError, MessagingErrorKind};
    use async_trait::async_trait;
    use mockall::{mock, Sequence};
    use tokio::sync::mpsc::error::SendError;

    mock! {
        NoopSender {}

        #[async_trait]
        impl Sender for NoopSender {
            type Data = TraceableDecision;
            pub async fn send(&self, value: TraceableDecision) -> Result<(), SendError<TraceableDecision>> {}
        }
    }
    mock! {
        NoopConsumer {}

        #[async_trait]
        impl Consumer for NoopConsumer {
            pub fn get_talos_type(&self) -> TalosType;
            pub async fn receive_message(&self) -> Result<ReceivedMessage, MessagingError>;
        }
    }

    #[tokio::test]
    async fn should_consume_and_forward() {
        let mut consumer = MockNoopConsumer::new();
        let mut destination = MockNoopSender::new();

        let response1_sample = ReceivedMessage::new_decision(make_decision("xid1".to_string()), 2, HashMap::new());
        let response2_sample = ReceivedMessage::new_decision(make_decision("xid2".to_string()), 2, HashMap::new());

        let decision1 = response1_sample.decision.clone().unwrap().decision.clone();
        let decision2 = response2_sample.decision.clone().unwrap().decision.clone();

        let mut seq = Sequence::new();
        consumer
            .expect_receive_message()
            .once()
            .in_sequence(&mut seq)
            .returning(move || Ok(response1_sample.clone()));

        destination
            .expect_send()
            .withf(move |param| param.decision.xid == decision1.xid)
            .once()
            .in_sequence(&mut seq)
            .returning(|_| Ok(()));

        consumer
            .expect_receive_message()
            .once()
            .in_sequence(&mut seq)
            .returning(move || Ok(response2_sample.clone()));

        destination
            .expect_send()
            .withf(move |param| param.decision.xid == decision2.xid)
            .once()
            .in_sequence(&mut seq)
            .returning(|_| Ok(()));

        let reader = DecisionReaderService::new(Arc::new(Box::new(consumer)), destination);
        let result = reader.read().await;
        assert!(result.is_ok());

        let result = reader.read().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_stop_reading_when_destination_is_closed() {
        let mut consumer = MockNoopConsumer::new();
        let mut destination = MockNoopSender::new();

        let response_sample = ReceivedMessage::new_decision(make_decision("xid1".to_string()), 2, HashMap::new());

        let response = response_sample.clone();
        let decision_to_send = response_sample.decision.unwrap().clone();

        consumer.expect_receive_message().once().returning(move || Ok(response.clone()));

        destination.expect_send().once().returning(move |_| Err(SendError(decision_to_send.clone())));

        let reader = DecisionReaderService::new(Arc::new(Box::new(consumer)), destination);
        reader.run().await;
    }

    #[tokio::test]
    async fn should_keep_reading_when_received_error() {
        let mut consumer = MockNoopConsumer::new();
        let mut destination = MockNoopSender::new();

        let response_sample = ReceivedMessage::new_decision(make_decision("xid1".to_string()), 2, HashMap::new());
        let response = response_sample.clone();
        let decision_to_send = response_sample.decision.unwrap();

        let mut seq = Sequence::new();

        // the first call returns an error
        consumer.expect_receive_message().once().in_sequence(&mut seq).returning(move || {
            Err(MessagingError {
                kind: MessagingErrorKind::Consuming,
                reason: "Simulating error".to_string(),
                cause: None,
            })
        });

        // the second call returns message
        consumer
            .expect_receive_message()
            .once()
            .in_sequence(&mut seq)
            .returning(move || Ok(response.clone()));

        // This recording is to interrupt the run loop
        destination
            .expect_send()
            .once()
            .in_sequence(&mut seq)
            .returning(move |_| Err(SendError(decision_to_send.clone())));

        let reader = DecisionReaderService::new(Arc::new(Box::new(consumer)), destination);
        reader.run().await;
    }

    fn make_decision(xid: String) -> DecisionMessage {
        DecisionMessage {
            xid,
            agent: String::from("agent"),
            cohort: String::from("cohort"),
            decision: Committed,
            suffix_start: 0_u64,
            version: 0_u64,
            safepoint: None,
            conflicts: None,
            metrics: None,
        }
    }
}
// $coverage:ignore-end
