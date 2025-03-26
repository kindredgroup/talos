#[derive(Clone, Debug, Default)]
pub struct ReplicatorOtelConfig {
    /**
     * See comment on cohort_sdk_js::models::JsCohortOtelConfig
     */
    pub init_otel: bool,
    pub enable_metrics: bool,
    pub enable_traces: bool,
    pub name: String,
    pub meter_name: String,
    // The endpoint to OTEL collector
    pub grpc_endpoint: Option<String>,
}
