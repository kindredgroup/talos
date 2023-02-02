use crate::api::{AgentConfig, CertificationRequest, CertificationResponse, TalosAgent};
use crate::messaging::api::{CandidateMessage, Publisher};
use async_trait::async_trait;

/// The implementation of agent.
pub struct TalosAgentImpl {
    pub config: AgentConfig,
    pub publisher: Box<dyn Publisher + Sync + Send>,
}

#[async_trait]
impl TalosAgent for TalosAgentImpl {
    async fn certify(&self, request: CertificationRequest) -> Result<CertificationResponse, String> {
        // Converts high level request to candidate message and sends it to the publisher
        let xid = request.candidate.xid.clone();
        let msg = CandidateMessage::new(self.config.agent_name.clone(), self.config.cohort_name.clone(), request.candidate);

        let publish_response = self.publisher.send_message(request.message_key.clone(), msg).await?;

        Ok(CertificationResponse {
            is_accepted: false,
            xid,
            partition: publish_response.partition,
            offset: publish_response.offset,
        })
    }
}
