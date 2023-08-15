use std::time::Duration;

use talos_agent::agent::errors::AgentError;

use super::{CertificationResponse, Conflict};

pub(crate) enum CertificationAttemptOutcome {
    Success { response: CertificationResponse },
    Aborted { response: CertificationResponse },
    AgentError { error: AgentError },
    DataError { reason: String },
    SnapshotTimeout { waited: Duration, conflict: Conflict },
}
