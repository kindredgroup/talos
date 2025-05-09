use std::time::Duration;

use strum::Display;
use talos_agent::agent::errors::AgentError;

use super::CertificationResponse;

#[derive(Clone)]
pub(crate) enum CertificationAttemptOutcome {
    Cancelled { reason: String },
    Success { response: CertificationResponse },
    Aborted { response: CertificationResponse },
    AgentError { error: AgentError },
    DataError { reason: String },
    SnapshotTimeout { waited: Duration, conflict: u64 },
}

#[derive(Display)]
pub(crate) enum CertificationAttemptFailure {
    Aborted,
    AgentError,
    Cancelled,
    Conflict,
    DataError,
    NotApplicable,
}

impl From<CertificationAttemptOutcome> for CertificationAttemptFailure {
    fn from(value: CertificationAttemptOutcome) -> Self {
        match value {
            CertificationAttemptOutcome::Aborted { response: _ } => CertificationAttemptFailure::Aborted,
            CertificationAttemptOutcome::AgentError { error: _ } => CertificationAttemptFailure::AgentError,
            CertificationAttemptOutcome::Cancelled { reason: _ } => CertificationAttemptFailure::Cancelled,
            CertificationAttemptOutcome::DataError { reason: _ } => CertificationAttemptFailure::DataError,
            CertificationAttemptOutcome::SnapshotTimeout { waited: _, conflict: _ } => CertificationAttemptFailure::Conflict,
            CertificationAttemptOutcome::Success { response: _ } => CertificationAttemptFailure::NotApplicable,
        }
    }
}
