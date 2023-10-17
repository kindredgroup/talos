use crate::models::{JsBackoffConfig, JsDecision, JsKafkaConfig};
use crate::sdk_errors::SdkErrorContainer;
use async_trait::async_trait;
use cohort_sdk::cohort::Cohort;
use cohort_sdk::model::callback::{
    CertificationCandidate, CertificationCandidateCallbackResponse, CertificationRequestPayload, OutOfOrderInstallOutcome, OutOfOrderInstallRequest,
    OutOfOrderInstaller,
};
use cohort_sdk::model::{CertificationResponse, ClientError, Config, ResponseMetadata};
use napi::bindgen_prelude::FromNapiValue;
use napi::bindgen_prelude::Promise;
use napi::bindgen_prelude::ToNapiValue;
use napi::threadsafe_function::ThreadsafeFunction;
use napi_derive::napi;
use serde_json::Value;
use std::collections::HashMap;

#[napi(object)]
pub struct JsInitiatorConfig {
    // cohort configs
    //
    pub backoff_on_conflict: JsBackoffConfig,
    pub retry_backoff: JsBackoffConfig,

    pub retry_attempts_max: u32,
    pub retry_oo_backoff: JsBackoffConfig,
    pub retry_oo_attempts_max: u32,

    pub snapshot_wait_timeout_ms: u32,

    //
    // agent config values
    //
    pub agent: String,
    pub cohort: String,
    // The size of internal buffer for candidates
    pub buffer_size: u32,
    pub timeout_ms: u32,

    pub kafka: JsKafkaConfig,
}

impl From<JsInitiatorConfig> for Config {
    fn from(val: JsInitiatorConfig) -> Self {
        Config {
            //
            // cohort configs
            //
            retry_attempts_max: val.retry_attempts_max,
            retry_backoff: val.retry_backoff.into(),
            backoff_on_conflict: val.backoff_on_conflict.into(),
            retry_oo_backoff: val.retry_oo_backoff.into(),
            retry_oo_attempts_max: val.retry_oo_attempts_max,
            snapshot_wait_timeout_ms: val.snapshot_wait_timeout_ms,

            //
            // agent config values
            //
            agent: val.agent,
            cohort: val.cohort,
            // The size of internal buffer for candidates
            buffer_size: val.buffer_size,
            timeout_ms: val.timeout_ms,

            kafka: val.kafka.into(),
        }
    }
}

#[napi(object)]
pub struct JsCertificationCandidate {
    pub readset: Vec<String>,
    pub writeset: Vec<String>,
    pub readvers: Vec<i64>,
    pub statemaps: Option<Vec<HashMap<String, Value>>>,
}

impl From<JsCertificationCandidate> for CertificationCandidate {
    fn from(val: JsCertificationCandidate) -> Self {
        CertificationCandidate {
            readset: val.readset,
            writeset: val.writeset,
            readvers: val.readvers.iter().map(|v| *v as u64).collect(),
            statemaps: val.statemaps,
        }
    }
}

#[napi(object)]
pub struct JsCertificationRequestPayload {
    pub candidate: JsCertificationCandidate,
    pub snapshot: i64,
    pub timeout_ms: u32,
}

impl From<JsCertificationRequestPayload> for CertificationRequestPayload {
    fn from(val: JsCertificationRequestPayload) -> Self {
        CertificationRequestPayload {
            candidate: val.candidate.into(),
            snapshot: val.snapshot as u64,
            timeout_ms: val.timeout_ms as u64,
        }
    }
}

#[napi(object)]
pub struct JsCertificationCandidateCallbackResponse {
    pub cancellation_reason: Option<String>,
    pub new_request: Option<JsCertificationRequestPayload>,
}

#[napi(string_enum)]
pub enum JsOutOfOrderInstallOutcome {
    Installed,
    InstalledAlready,
    SafepointCondition,
}

impl From<JsOutOfOrderInstallOutcome> for OutOfOrderInstallOutcome {
    fn from(value: JsOutOfOrderInstallOutcome) -> Self {
        match value {
            JsOutOfOrderInstallOutcome::Installed => OutOfOrderInstallOutcome::Installed,
            JsOutOfOrderInstallOutcome::SafepointCondition => OutOfOrderInstallOutcome::SafepointCondition,
            JsOutOfOrderInstallOutcome::InstalledAlready => OutOfOrderInstallOutcome::InstalledAlready,
        }
    }
}

#[napi(object)]
pub struct JsCertificationResponse {
    pub xid: String,
    pub decision: JsDecision,
    pub version: i64,
    pub safepoint: Option<i64>,
    pub conflict: Option<i64>,
    pub metadata: JsResponseMetadata,
    pub statemaps: Option<Vec<HashMap<String, Value>>>,
}

impl From<CertificationResponse> for JsCertificationResponse {
    fn from(value: CertificationResponse) -> Self {
        Self {
            xid: value.xid,
            decision: value.decision.into(),
            version: value.version as i64,
            safepoint: value.safepoint.map(|v| v as i64),
            conflict: value.conflict.map(|v| v as i64),
            metadata: value.metadata.into(),
            statemaps: value.statemaps,
        }
    }
}

impl From<ResponseMetadata> for JsResponseMetadata {
    fn from(value: ResponseMetadata) -> Self {
        Self {
            attempts: value.attempts,
            duration_ms: value.duration_ms as i64,
        }
    }
}

#[napi(object)]
pub struct JsResponseMetadata {
    pub attempts: u32,
    pub duration_ms: i64,
}

#[napi]
pub struct InternalInitiator {
    cohort: Cohort,
}

#[napi]
impl InternalInitiator {
    #[napi]
    pub async fn init(config: JsInitiatorConfig) -> napi::Result<InternalInitiator> {
        let cohort = Cohort::create(config.into()).await.map_err(map_error)?;
        Ok(InternalInitiator { cohort })
    }

    #[napi]
    pub async fn certify(
        &self,
        #[napi(ts_arg_type = "() => Promise<JsCertificationCandidateCallbackResponse>")] make_new_request_callback: ThreadsafeFunction<()>,
        #[napi(ts_arg_type = "(error: Error | null, ooRequest: OutOfOrderRequest) => Promise<JsOutOfOrderInstallOutcome>")] ooo_callback: ThreadsafeFunction<
            OutOfOrderRequest,
        >,
    ) -> napi::Result<JsCertificationResponse> {
        let new_request_provider = NewRequestProvider { make_new_request_callback };
        let ooo_impl = OutOfOrderInstallerImpl { ooo_callback };
        let make_new_request = || new_request_provider.make_new_request();
        let response = self.cohort.certify(&make_new_request, &ooo_impl).await.map_err(map_error)?;
        Ok(response.into())
    }
}

struct OutOfOrderInstallerImpl {
    ooo_callback: ThreadsafeFunction<OutOfOrderRequest>,
}

#[async_trait]
impl OutOfOrderInstaller for OutOfOrderInstallerImpl {
    async fn install(&self, request: OutOfOrderInstallRequest) -> Result<OutOfOrderInstallOutcome, String> {
        let oorequest = OutOfOrderRequest {
            xid: request.xid,
            safepoint: request.safepoint.try_into().unwrap(),
            new_version: request.version.try_into().unwrap(),
        };

        let result = self.ooo_callback.call_async::<Promise<JsOutOfOrderInstallOutcome>>(Ok(oorequest)).await;

        match result {
            Ok(promise) => promise
                .await
                .map(|outcome| outcome.into())
                .map_err(|e| format!("Unable to install out of orer item. Native reason provided by JS: \"{}\"", e.reason)),
            Err(e) => Err(e.to_string()),
        }
    }
}

pub struct NewRequestProvider {
    make_new_request_callback: ThreadsafeFunction<()>,
}

impl NewRequestProvider {
    async fn make_new_request(&self) -> Result<CertificationCandidateCallbackResponse, String> {
        let result = self
            .make_new_request_callback
            .call_async::<Promise<JsCertificationCandidateCallbackResponse>>(Ok(()))
            .await;

        match result {
            Ok(promise) => promise
                .await
                .map(|js_data: JsCertificationCandidateCallbackResponse| {
                    if js_data.cancellation_reason.is_some() {
                        CertificationCandidateCallbackResponse::Cancelled(js_data.cancellation_reason.unwrap())
                    } else {
                        CertificationCandidateCallbackResponse::Proceed(
                            js_data
                                .new_request
                                .expect(
                                    "Invalid response from 'make_new_request_callback'. Provide cancellation reason or new request. Currently both are empty.",
                                )
                                .into(),
                        )
                    }
                })
                // Here reason is empty with NAPI 2.10.3
                .map_err(|e| format!("Unable to create new certification request. Native reason reported from JS: \"{}\"", e.reason)),

            Err(e) => Err(e.to_string()),
        }
    }
}

#[napi(object)]
pub struct OutOfOrderRequest {
    pub xid: String,
    pub safepoint: i64,
    pub new_version: i64,
}

fn map_error(e: ClientError) -> napi::Error {
    let container = SdkErrorContainer::new(e.kind.into(), e.reason, e.cause);
    napi::Error::from_reason(container.json().to_string())
}
