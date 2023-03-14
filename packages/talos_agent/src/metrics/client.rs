use crate::metrics::model::{Event, EventName, Signal};
use time::OffsetDateTime;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

/// Clone-friendly service which can be used to transmit signal into metrics system.
pub struct MetricsClient {
    pub tx_destination: Sender<Signal>,
}

impl MetricsClient {
    /// Makes an instance of new event with current timestamp and transmits it to metrics system
    pub fn new_event(&self, name: EventName, id: String) -> JoinHandle<Result<u64, SendError<Signal>>> {
        self.new_event_at(name, id, OffsetDateTime::now_utc().unix_timestamp_nanos() as u64)
    }

    /// Makes an instance of new event with the given timestamp and transmits it to metrics system
    pub fn new_event_at(&self, name: EventName, id: String, time: u64) -> JoinHandle<Result<u64, SendError<Signal>>> {
        let tx = self.tx_destination.clone();
        tokio::spawn(async move {
            let event = Event {
                event_name: name,
                time,
                id: id.clone(),
            };

            match tx.send(Signal::Start { time, event }).await {
                Ok(()) => Ok(time),
                Err(e) => Err(e),
            }
        })
    }

    pub async fn _end_span(self, id: String, name: EventName) -> JoinHandle<Result<u64, SendError<Signal>>> {
        let now = OffsetDateTime::now_utc().unix_timestamp_nanos() as u64;
        let tx = self.tx_destination;
        tokio::spawn(async move {
            match tx
                .send(Signal::End {
                    id: id.clone(),
                    event_name: name,
                    time: now,
                })
                .await
            {
                Ok(()) => Ok(now),
                Err(e) => Err(e),
            }
        })
    }
}