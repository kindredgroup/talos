use crate::api::{CertificationRequest, CertificationResponse};
use crate::mpsc::core::Sender;
use std::fmt::Debug;
use std::sync::Arc;

// Sent by agent to state manager
#[derive(Clone)]
pub struct CertifyRequestChannelMessage {
    pub request: CertificationRequest,
    pub tx_answer: Arc<Box<dyn Sender<Data = CertificationResponse>>>,
}

impl Debug for CertifyRequestChannelMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CertifyRequestChannelMessage: [request: {:?}]", self.request)
    }
}

impl CertifyRequestChannelMessage {
    pub fn new(request: &CertificationRequest, tx_answer: Arc<Box<dyn Sender<Data = CertificationResponse>>>) -> CertifyRequestChannelMessage {
        CertifyRequestChannelMessage {
            request: request.clone(),
            tx_answer,
        }
    }
}

// Sent by agent to state manager
#[derive(Debug, Clone)]
pub struct CancelRequestChannelMessage {
    pub request: CertificationRequest,
}
