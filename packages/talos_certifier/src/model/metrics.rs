use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TxProcessingTimeline {
    pub candidate_published: i128,
    pub candidate_received: i128,
    pub candidate_processing_started: i128,
    pub decision_created_at: i128,
    pub db_save_started: i128,
    pub db_save_ended: i128,
    #[serde(skip_serializing, skip_deserializing)]
    pub decision_received_at: i128,
}
