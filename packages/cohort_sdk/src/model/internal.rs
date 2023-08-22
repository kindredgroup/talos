use std::time::Duration;

use talos_agent::agent::errors::AgentError;

use super::CertificationResponse;

pub(crate) enum CertificationAttemptOutcome {
    ClientAborted { reason: String },
    Success { response: CertificationResponse },
    Aborted { response: CertificationResponse },
    AgentError { error: AgentError },
    DataError { reason: String },
    SnapshotTimeout { waited: Duration, conflict: u64 },
}
