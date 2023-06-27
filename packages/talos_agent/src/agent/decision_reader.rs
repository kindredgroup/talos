use crate::messaging::api::{ConsumerType, DecisionMessage};
use crate::mpsc::core::Sender;
use std::sync::Arc;

pub struct DecisionReaderService<S: Sender<Data = DecisionMessage>> {
    consumer: Arc<Box<ConsumerType>>,
    tx_decision: S,
}

impl<S: Sender<Data = DecisionMessage>> DecisionReaderService<S> {
    pub fn new(consumer: Arc<Box<ConsumerType>>, tx_decision: S) -> Self {
        DecisionReaderService { consumer, tx_decision }
    }

    pub async fn run(&self) {
        loop {
            if let Err(ReaderError::ChannelIsClosed { e: reason }) = self.read().await {
                log::error!("Destination channel is closed: {}", reason);
                break;
            }
        }
    }

    async fn read(&self) -> Result<(), ReaderError> {
        match self.consumer.receive_message().await {
            Some(Ok(decision_msg)) => Ok(self
                .tx_decision
                .send(decision_msg)
                .await
                .map_err(|send_error| ReaderError::ChannelIsClosed { e: format!("{}", send_error) })?),

            Some(Err(_)) => Err(ReaderError::ConsumerError),

            None => Ok(()),
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
    use super::*;
    use crate::messaging::api::Consumer;
    use crate::messaging::api::Decision::Committed;
    use crate::messaging::errors::{MessagingError, MessagingErrorKind};
    use async_trait::async_trait;
    use mockall::{mock, Sequence};
    use tokio::sync::mpsc::error::SendError;

    mock! {
        NoopSender {}

        #[async_trait]
        impl Sender for NoopSender {
            type Data = DecisionMessage;
            pub async fn send(&self, value: DecisionMessage) -> Result<(), SendError<DecisionMessage>> {}
        }
    }
    mock! {
        NoopConsumer {}

        #[async_trait]
        impl Consumer for NoopConsumer {
            pub async fn receive_message(&self) -> Option<Result<DecisionMessage, MessagingError>>;
        }
    }

    #[tokio::test]
    async fn should_consume_and_forward() {
        let mut consumer = MockNoopConsumer::new();
        let mut destination = MockNoopSender::new();

        let decision1_sample = make_decision("xid1".to_string());
        let decision2_sample = make_decision("xid2".to_string());

        let decision1 = decision1_sample.clone();
        let decision2 = decision2_sample.clone();

        let mut seq = Sequence::new();
        consumer
            .expect_receive_message()
            .once()
            .in_sequence(&mut seq)
            .returning(move || Some(Ok(decision1_sample.clone())));

        destination
            .expect_send()
            .withf(move |param| param.xid == decision1.xid)
            .once()
            .in_sequence(&mut seq)
            .returning(|_| Ok(()));

        consumer
            .expect_receive_message()
            .once()
            .in_sequence(&mut seq)
            .returning(move || Some(Ok(decision2_sample.clone())));

        destination
            .expect_send()
            .withf(move |param| param.xid == decision2.xid)
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

        let decision_sample = make_decision("xid1".to_string());

        let decision_copy1 = decision_sample.clone();
        let decision_copy2 = decision_sample.clone();

        consumer.expect_receive_message().once().returning(move || Some(Ok(decision_copy1.clone())));

        destination.expect_send().once().returning(move |_| Err(SendError(decision_copy2.clone())));

        let reader = DecisionReaderService::new(Arc::new(Box::new(consumer)), destination);
        reader.run().await;
    }

    #[tokio::test]
    async fn should_keep_reading_when_received_none() {
        let mut consumer = MockNoopConsumer::new();
        let mut destination = MockNoopSender::new();
        let decision_sample = make_decision("xid1".to_string());
        let decision1 = decision_sample.clone();
        let decision2 = decision_sample.clone();

        let mut seq = Sequence::new();
        consumer.expect_receive_message().once().in_sequence(&mut seq).returning(move || None);

        consumer
            .expect_receive_message()
            .once()
            .in_sequence(&mut seq)
            .returning(move || Some(Ok(decision1.clone())));

        destination
            .expect_send()
            .withf(move |param| param.xid == decision2.xid)
            .once()
            .returning(move |_| Ok(()));

        let reader = DecisionReaderService::new(Arc::new(Box::new(consumer)), destination);
        let result = reader.read().await;
        assert!(result.is_ok());
        let result = reader.read().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_keep_reading_when_received_error() {
        let mut consumer = MockNoopConsumer::new();
        let mut destination = MockNoopSender::new();

        let decision_sample = make_decision("xid1".to_string());
        let decision_copy1 = decision_sample.clone();
        let decision_copy2 = decision_sample.clone();

        let mut seq = Sequence::new();

        // the first call returns an error
        consumer.expect_receive_message().once().in_sequence(&mut seq).returning(move || {
            Some(Err(MessagingError {
                kind: MessagingErrorKind::Consuming,
                reason: "Simulating error".to_string(),
                cause: None,
            }))
        });

        // the second call returns message
        consumer
            .expect_receive_message()
            .once()
            .in_sequence(&mut seq)
            .returning(move || Some(Ok(decision_copy1.clone())));

        // This recording is to interrupt the run loop
        destination
            .expect_send()
            .once()
            .in_sequence(&mut seq)
            .returning(move |_| Err(SendError(decision_copy2.clone())));

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
            can_received_at: None,
            created_at: None,
        }
    }
}
// $coverage:ignore-end
