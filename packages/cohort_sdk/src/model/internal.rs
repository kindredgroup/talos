use talos_agent::agent::errors::AgentError;

use super::CertificationResponse;

pub(crate) enum CertificationAttemptOutcome {
    Success { response: CertificationResponse },
    Aborted { response: CertificationResponse },
    AgentError { error: AgentError },
    DataError { reason: String },
}
