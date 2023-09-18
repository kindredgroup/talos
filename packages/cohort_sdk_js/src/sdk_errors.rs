use cohort_sdk::model::ClientErrorKind;
use napi::bindgen_prelude::FromNapiValue;
use napi::bindgen_prelude::ToNapiValue;
use napi_derive::napi;
use serde::Serialize;
use serde_json::Value;

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

#[derive(Serialize)]
#[napi]
pub struct SdkErrorContainer {
    /// Should always hold the name of this class. This attribute is used
    /// by JS layer to recognise out container and safely parse it from JSON string
    /// back to structured object.
    pub typ: String,
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
        Self {
            typ: SDK_CONTAINER_TYPE.into(),
            kind,
            reason,
            cause,
        }
    }

    pub fn json(&self) -> Value {
        serde_json::to_value(self).unwrap()
    }
}
