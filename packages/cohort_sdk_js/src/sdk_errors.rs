use cohort_sdk::model::ClientErrorKind;
use napi_derive::napi;
use serde::Serialize;
use serde_json::Value;
use talos_cohort_replicator::errors::ReplicatorErrorKind;

#[napi(string_enum)]
#[derive(Serialize)]
// this is napi friendly copy of cohort_sdk::model::ClientErrorKind
pub enum SdkErrorKind {
    Certification,
    CertificationTimeout,
    Cancelled,
    Messaging,
    Persistence,
    Internal,
    OutOfOrderCallbackFailed,
    OutOfOrderSnapshotTimeout,
}

impl From<ClientErrorKind> for SdkErrorKind {
    fn from(value: ClientErrorKind) -> Self {
        match value {
            ClientErrorKind::Certification => SdkErrorKind::Certification,
            ClientErrorKind::CertificationTimeout => SdkErrorKind::CertificationTimeout,
            ClientErrorKind::Cancelled => SdkErrorKind::Cancelled,
            ClientErrorKind::Messaging => SdkErrorKind::Messaging,
            ClientErrorKind::Persistence => SdkErrorKind::Persistence,
            ClientErrorKind::Internal => SdkErrorKind::Internal,
            ClientErrorKind::OutOfOrderCallbackFailed => SdkErrorKind::OutOfOrderCallbackFailed,
            ClientErrorKind::OutOfOrderSnapshotTimeout => SdkErrorKind::OutOfOrderSnapshotTimeout,
        }
    }
}

impl From<ReplicatorErrorKind> for SdkErrorKind {
    fn from(value: ReplicatorErrorKind) -> Self {
        match value {
            // will never happen in cohort
            ReplicatorErrorKind::Internal => SdkErrorKind::Internal,
            ReplicatorErrorKind::Messaging => SdkErrorKind::Messaging,
            ReplicatorErrorKind::Persistence => SdkErrorKind::Persistence,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase", tag = "_typ")]
#[napi]
pub struct SdkErrorContainer {
    pub kind: SdkErrorKind,
    pub reason: String,
    pub cause: Option<String>,
}

#[napi]
pub const SDK_CONTAINER_TYPE: &str = "SdkErrorContainer";

/// The purpose of this class is to transfer structured error with some business code from
/// Rust world to JS. When "napi::Error.reason" is used as transfer container then instance of this
/// class should be encoded into string. For simplicity we provide a helper method json(self).
#[napi]
impl SdkErrorContainer {
    pub fn new(kind: SdkErrorKind, reason: String, cause: Option<String>) -> Self {
        Self { kind, reason, cause }
    }

    pub fn json(&self) -> Value {
        serde_json::to_value(self).unwrap()
    }
}
