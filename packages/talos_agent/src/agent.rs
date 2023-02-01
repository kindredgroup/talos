use crate::api::{AgentConfig, CertificationRequest, CertificationResponse, TalosAgent};
use crate::messaging::api::{CandidateMessage, Publisher};

/// The implementation of agent.
pub struct TalosAgentImpl {
    pub config: AgentConfig,
    pub publisher: Box<dyn Publisher>,
}

impl TalosAgent for TalosAgentImpl {
    fn certify(&self, request: CertificationRequest) -> Result<CertificationResponse, String> {
        // Converts high level request to candidate message and sends it to the publisher
        let xid = request.candidate.xid.clone();
        let msg = CandidateMessage::new(
            self.config.agent_name.clone(),
            self.config.cohort_name.clone(),
            request.candidate);

        let rsp = self.publisher.send_message(request.message_key.clone(), msg);
        let result = match rsp {
            Ok(publish_response) => {
                Ok(CertificationResponse {
                    is_accepted: false,
                    xid,
                    partition: publish_response.partition,
                    offset: publish_response.offset })
            },
            Err(e) => Err(e)
        };

        return result;
    }
}

