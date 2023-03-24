use crate::metrics::model::{Event, EventName, Signal};
use time::OffsetDateTime;
use tokio::sync::mpsc::error::SendError;

/// Clone-friendly service which can be used to transmit signal into metrics system.
pub struct MetricsClient<TSignalTx: crate::mpsc::core::Sender<Data = Signal>> {
    pub tx_destination: TSignalTx,
}

impl<TSignalTx: crate::mpsc::core::Sender<Data = Signal> + 'static> MetricsClient<TSignalTx> {
    /// Makes an instance of new event with current timestamp and transmits it to metrics system
    pub async fn new_event(&self, name: EventName, id: String) -> Result<u64, SendError<Signal>> {
        self.new_event_at(name, id, OffsetDateTime::now_utc().unix_timestamp_nanos() as u64).await
    }

    /// Makes an instance of new event with the given timestamp and transmits it to metrics system
    pub async fn new_event_at(&self, name: EventName, id: String, time: u64) -> Result<u64, SendError<Signal>> {
        let event = Event {
            event_name: name,
            time,
            id: id.clone(),
        };
        match self.tx_destination.send(Signal::Start { time, event }).await {
            Ok(()) => Ok(time),
            Err(e) => Err(e),
        }
    }
}

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use super::*;
    use crate::mpsc::core::Sender;
    use async_trait::async_trait;
    use mockall::mock;

    mock! {
        NoopSender {}

        #[async_trait]
        impl Sender for NoopSender {
            type Data = Signal;

            pub async fn send(&self, value: Signal) -> Result<(), SendError<Signal>> {}
        }
    }

    #[tokio::test]
    async fn new_event_at_should_send_start_signal() {
        let mut tx_destination = MockNoopSender::new();
        tx_destination
            .expect_send()
            .withf(move |param| {
                let event = Event {
                    id: "xid1".to_string(),
                    event_name: EventName::Started,
                    time: 1111,
                };
                *param == Signal::Start { time: 1111, event }
            })
            .once()
            .returning(move |_| Ok(()));

        let client = MetricsClient { tx_destination };
        let result = client.new_event_at(EventName::Started, "xid1".to_string(), 1111).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn new_event_should_send_start_signal() {
        let mut tx_destination = MockNoopSender::new();
        tx_destination
            .expect_send()
            .withf(move |param| {
                if let Signal::Start { time, event } = param {
                    *time > 0 && event.event_name == EventName::Started && event.id == *"xid1"
                } else {
                    false
                }
            })
            .once()
            .returning(move |_| Ok(()));

        let client = MetricsClient { tx_destination };
        let result = client.new_event_at(EventName::Started, "xid1".to_string(), 1111).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn new_event_should_propagate_send_error() {
        let mut tx_destination = MockNoopSender::new();
        tx_destination.expect_send().returning(move |e| Err(SendError(e)));

        let client = MetricsClient { tx_destination };
        let result = client.new_event_at(EventName::Started, "xid1".to_string(), 1111).await;
        assert!(result.is_err());
    }
}
// $coverage:ignore-end
