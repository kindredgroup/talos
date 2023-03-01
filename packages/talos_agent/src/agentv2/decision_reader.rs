use crate::messaging::api::{ConsumerType, DecisionMessage};
use tokio::sync::mpsc::Sender;

pub struct DecisionReaderService {
    consumer: Box<ConsumerType>,
    tx_decision: Sender<DecisionMessage>,
}

impl DecisionReaderService {
    pub fn new(consumer: Box<ConsumerType>, tx_decision: Sender<DecisionMessage>) -> Self {
        DecisionReaderService { consumer, tx_decision }
    }

    pub async fn run(&self) {
        loop {
            let rslt_decision_msg = self.consumer.receive_message().await;
            match rslt_decision_msg {
                Some(Ok(decision_msg)) => {
                    match self.tx_decision.send(decision_msg).await {
                        Ok(()) => continue,
                        Err(e) => {
                            log::error!("Unable to send the decision message via internal channel {:?}", e);
                        }
                    };
                }

                _ => continue, // just take next message
            }
        }
    }
}
