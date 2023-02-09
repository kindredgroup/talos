use crate::api::{AgentConfig, CertificationRequest, CertificationResponse, TalosAgent};
use crate::messaging::api::{CandidateMessage, ConsumerType, Decision, PublisherType};
use async_trait::async_trait;
use log::error;

/// The implementation of agent.
pub struct TalosAgentImpl {
    pub config: AgentConfig,
    pub publisher: Box<PublisherType>,
    pub consumer: Box<ConsumerType>,
}

#[async_trait]
impl TalosAgent for TalosAgentImpl {
    async fn certify(&self, request: CertificationRequest) -> Result<CertificationResponse, String> {
        // Converts high level request to candidate message and sends it to the publisher
        let xid = request.candidate.xid.clone();
        let msg = CandidateMessage::new(self.config.agent_name.clone(), self.config.cohort_name.clone(), request.candidate);

        let _publish_response = self.publisher.send_message(request.message_key.clone(), msg).await?;

        let mut polled_total = 0;
        let mut polled_empty = 0;
        let mut polled_others = 0;
        loop {
            polled_total += 1;
            let res = self.consumer.receive_message().await;
            match res {
                None => {
                    polled_empty += 1;
                    continue;
                }
                Some(Err(e)) => {
                    error!("certify(): error receiving: {}", e);
                    return Err(e);
                }
                Some(Ok(message_received)) => {
                    if xid != message_received.xid {
                        polled_others += 1;
                        continue;
                    }

                    return Ok(CertificationResponse {
                        xid: message_received.xid,
                        is_accepted: message_received.decision == Decision::Committed,
                        polled_total,
                        polled_empty,
                        polled_others,
                    });
                }
            }
        }
    }
}
