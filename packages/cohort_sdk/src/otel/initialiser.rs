use talos_common_utils::otel::initialiser::OtelInitError;

use crate::model::ClientError;

impl From<OtelInitError> for ClientError {
    fn from(value: OtelInitError) -> Self {
        Self {
            kind: crate::model::ClientErrorKind::Internal,
            cause: Some(value.to_string()),
            reason: "Error initialising OTEL".into(),
        }
    }
}
