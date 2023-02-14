use crate::api::{CertificationRequest, CertificationResponse};
use tokio::sync::mpsc::Sender;

// Sent by agent to state manager
pub struct CertifyRequestChannelMessage {
    pub request: CertificationRequest,
    pub tx_answer: Sender<CertificationResponse>,
}

impl CertifyRequestChannelMessage {
    pub fn new(request: &CertificationRequest, tx_answer: &Sender<CertificationResponse>) -> CertifyRequestChannelMessage {
        CertifyRequestChannelMessage {
            request: request.clone(),
            tx_answer: tx_answer.clone(),
        }
    }
}

// Sent by agent to state manager
pub struct CancelRequestChannelMessage {
    pub request: CertificationRequest,
}
