use opentelemetry::Context;

use crate::api::{CertificationRequest, CertificationResponse};
use crate::mpsc::core::Sender;
use std::sync::Arc;

// Sent by agent to state manager
#[derive(Clone)]
pub struct CertifyRequestChannelMessage {
    pub request: CertificationRequest,
    pub tx_answer: Arc<Box<dyn Sender<Data = CertificationResponse>>>,
    pub parent_span: Option<(Context, tracing::Id)>,
}

impl CertifyRequestChannelMessage {
    pub fn new(
        request: &CertificationRequest,
        tx_answer: Arc<Box<dyn Sender<Data = CertificationResponse>>>,
        parent_span: Option<(Context, tracing::Id)>,
    ) -> CertifyRequestChannelMessage {
        CertifyRequestChannelMessage {
            request: request.clone(),
            tx_answer,
            parent_span,
        }
    }
}

// Sent by agent to state manager
#[derive(Debug, Clone)]
pub struct CancelRequestChannelMessage {
    pub request: CertificationRequest,
}
