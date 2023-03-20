use crate::messaging::api::{ConsumerType, DecisionMessage};
use std::sync::Arc;
use crate::mpsc::core::Sender;

pub struct DecisionReaderService<S: Sender<Data=DecisionMessage>> {
    consumer: Arc<Box<ConsumerType>>,
    tx_decision: S,
}

impl <S: Sender<Data=DecisionMessage>> DecisionReaderService<S> {
    pub fn new(consumer: Arc<Box<ConsumerType>>, tx_decision: S) -> Self {
        DecisionReaderService {
            consumer,
            tx_decision
        }
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
