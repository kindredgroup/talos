// prefix it with relevant sybsystem: repl_ or cohort_ or certifier_ or messenger_

pub const METRIC_METER_NAME_COHORT_SDK: &str = "cohort_sdk";
pub const METRIC_NAME_AGENT_OFFSET_LAG: &str = "agent_offset_lag";
pub const METRIC_NAME_CERTIFICATION_OFFSET: &str = "certification_offset";
pub const METRIC_KEY_CERT_MESSAGE_TYPE: &str = "message_type";
pub const METRIC_KEY_IS_SUCCESS: &str = "is_success";
pub const METRIC_KEY_REASON: &str = "reason";
pub const METRIC_VALUE_CERT_MESSAGE_TYPE_CANDIDATE: &str = "Candidate";
pub const METRIC_VALUE_CERT_MESSAGE_TYPE_DECISION: &str = "Decision";
pub const METRIC_KEY_CERT_DECISION_TYPE: &str = "decision";
// used as attribute when reporting certification message offset
pub const METRIC_VALUE_CERT_DECISION_TYPE_UNKNOWN: &str = "Unknown";
//        METRIC_VALUE_CERT_DECISION_TYPE_FOR_DECISION // just serialise Decision num, its eitier "Commit" or "Abort"
