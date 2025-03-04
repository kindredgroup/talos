use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::{global, Context};

use crate::api::{CertificationRequest, CertificationResponse};
use crate::mpsc::core::Sender;
use std::collections::HashMap;
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

#[derive(Default)]
pub struct PropagatedSpanContextData {
    data: HashMap<String, String>,
}

impl PropagatedSpanContextData {
    pub fn new_with_trace_parent(trace_parent: String) -> Self {
        Self::new_with_data(HashMap::from([(String::from("traceparent"), trace_parent)]))
    }

    pub fn new_with_data(data: HashMap<String, String>) -> Self {
        Self { data }
    }

    pub fn get_data(self) -> HashMap<String, String> {
        self.data
    }

    pub fn new_with_otel_context(otel_context: &opentelemetry::Context) -> Self {
        global::get_text_map_propagator(|p| {
            let mut ctx_data = PropagatedSpanContextData::default();
            p.inject_context(otel_context, &mut ctx_data);
            ctx_data
        })
    }
}

impl Injector for PropagatedSpanContextData {
    fn set(&mut self, key: &str, value: String) {
        self.data.insert(key.to_owned(), value);
    }
}

impl Extractor for PropagatedSpanContextData {
    fn get(&self, key: &str) -> Option<&str> {
        let key = key.to_owned();
        self.data.get(&key).map(|v| v.as_ref())
    }

    fn keys(&self) -> Vec<&str> {
        self.data.keys().map(|k| k.as_ref()).collect()
    }
}
