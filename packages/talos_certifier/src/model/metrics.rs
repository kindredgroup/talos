use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TxProcessingTimeline {
    /// Cohort started certification
    #[serde(default)]
    pub certification_started: i128,
    /// The request for certification is created
    #[serde(default)]
    pub request_created: i128,
    /// Candidate published to kafka (agent time)
    pub candidate_published: i128,
    /// Candidate received by Certifier
    pub candidate_received: i128,
    pub candidate_processing_started: i128,
    pub decision_created_at: i128,
    pub db_save_started: i128,
    pub db_save_ended: i128,
    #[serde(skip_serializing, skip_deserializing)]
    pub decision_received_at: i128,
}

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use crate::model::metrics::TxProcessingTimeline;

    #[test]
    fn deserialise_using_defaults() {
        let json = r#"{
            "candidatePublished": 1,
            "candidateReceived": 2,
            "candidateProcessingStarted": 3,
            "decisionCreatedAt": 4,
            "dbSaveStarted": 5,
            "dbSaveEnded": 6
        }"#;

        let deserialised: TxProcessingTimeline = serde_json::from_str(json).unwrap();
        assert_eq!(deserialised.certification_started, 0);
        assert_eq!(deserialised.request_created, 0);
        assert_eq!(deserialised.candidate_published, 1);
        assert_eq!(deserialised.candidate_received, 2);
        assert_eq!(deserialised.candidate_processing_started, 3);
        assert_eq!(deserialised.decision_created_at, 4);
        assert_eq!(deserialised.db_save_started, 5);
        assert_eq!(deserialised.db_save_ended, 6);
    }
}
