use std::time::Duration;

use async_trait::async_trait;
use log::{debug, info};

use tokio::sync::mpsc;

use crate::{
    core::{MessengerChannelFeedback, MessengerSystemService},
    errors::MessengerServiceResult,
};

pub struct MessengerFeedbackService {
    pub tx_feedback_batch_channel: mpsc::Sender<Vec<MessengerChannelFeedback>>,
    pub rx_feedback_channel: mpsc::Receiver<MessengerChannelFeedback>,
}

#[async_trait]
impl MessengerSystemService for MessengerFeedbackService {
    async fn start(&self) -> MessengerServiceResult {
        info!("Start Messenger Feedback service");
        Ok(())
    }

    async fn stop(&self) -> MessengerServiceResult {
        Ok(())
    }

    async fn run(&mut self) -> MessengerServiceResult {
        info!("Running Messenger Feedback service");
        let mut message_feedbacks: Vec<MessengerChannelFeedback> = vec![];
        let mut interval = tokio::time::interval(Duration::from_millis(50));

        loop {
            tokio::select! {

                // Receive feedback from publisher.
                feedback_result = self.rx_feedback_channel.recv() => {
                    match feedback_result {
                        Some(res) => {
                           message_feedbacks.push(res)

                        },
                        None => {
                            debug!("No feedback message to process..");
                        }
                    }

                }
                // Push the batch of messages
                _ = interval.tick() => {
                    let message_feedback_batch = message_feedbacks.drain(..).collect::<Vec<MessengerChannelFeedback>>();
                    let batch_length = message_feedback_batch.len();
                    if batch_length > 0 {
                        let _res = self.tx_feedback_batch_channel.send(message_feedback_batch).await;
                        // warn!("Result for sending... {res:?} for {} feedbacks", batch_length);
                    }
                }
            }
        }
    }
}
