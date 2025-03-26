use std::collections::HashMap;

use opentelemetry::global;
use opentelemetry::propagation::{Extractor, Injector};

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
