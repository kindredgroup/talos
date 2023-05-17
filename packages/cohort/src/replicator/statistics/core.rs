#[derive(Debug, Default, Clone)]
pub struct ReplicatorStatisticsItem {
    pub version: u64,
    // Candidate related
    pub candidate_received_time: Option<u128>,

    // decision related
    pub decision_received_time: Option<u128>,
    pub is_committed_decision: Option<bool>,

    // suffix related
    pub suffix_insert_candidate_time: Option<u128>,
    pub suffix_decision_update_time: Option<u128>,
    pub suffix_update_install_flag: Option<u128>,

    // statemap related
    pub statemap_batch_create_time: Option<u128>,
    pub statemap_install_time: Option<u128>,
    pub statemap_install_retries: u32,
    pub is_statemap_install_success: Option<bool>,
}

pub enum ReplicatorStatisticsChannelMessage {
    CandidateReceivedTime(u64, u128),
    DecisionReceivedTime(u64, u128),

    SuffixInsertCandidate(u64, u128),
    SuffixUpdateDecision(u64, u128, bool),
    SuffixUpdateInstallFlagsTime(u64, u128),

    StatemapBatchCreateTime(u64, u128),
    StatemapInstallationTime(u64, u128, bool),
    // StatemapInstallRetries(u64),
}
