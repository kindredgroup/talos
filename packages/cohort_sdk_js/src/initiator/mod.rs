use crate::map_error_to_napi_error;
use crate::models::{JsBackoffConfig, JsKafkaConfig};
use async_trait::async_trait;
use cohort_sdk::cohort::Cohort;
use cohort_sdk::model::callback::{
    CertificationCandidate, CertificationCandidateCallbackResponse, CertificationRequestPayload, OutOfOrderInstallOutcome, OutOfOrderInstallRequest,
    OutOfOrderInstaller,
};
use cohort_sdk::model::Config;
use napi::bindgen_prelude::Promise;
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

#[napi]
pub struct Initiator {
    cohort: Cohort,
}

#[napi]
impl Initiator {
    #[napi]
    pub async fn init(config: JsInitiatorConfig) -> napi::Result<Initiator> {
        let cohort = Cohort::create(config.into()).await.map_err(map_error_to_napi_error)?;
        Ok(Initiator { cohort })
    }

    #[napi]
    pub async fn certify(
        &self,
        #[napi(ts_arg_type = "() => Promise<any>")] make_new_request_callback: ThreadsafeFunction<()>,
        ooo_callback: ThreadsafeFunction<OutOfOrderRequest>,
    ) -> napi::Result<String> {
        // println!("Initiator.certify()");
        let new_request_provider = NewRequestProvider { make_new_request_callback };
        let ooo_impl = OutOfOrderInstallerImpl { ooo_callback };
        // println!("Initiator.certify(): invoking cohort.certify(...)");
        let make_new_request = || new_request_provider.make_new_request();
        let _res = self.cohort.certify(&make_new_request, &ooo_impl).await.map_err(map_error_to_napi_error)?;

        // println!("Initiator.certify(): after cohort.certify(...)");
        Ok("Success".to_string())
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

        let result = self
            .ooo_callback
            .call_async::<Promise<i64>>(Ok(oorequest))
            .await
            .map_err(map_error_to_napi_error);
        match result {
            Ok(promise) => promise
                .await
                .map(|code| match code {
                    1 => OutOfOrderInstallOutcome::InstalledAlready,
                    2 => OutOfOrderInstallOutcome::SafepointCondition,
                    _ => OutOfOrderInstallOutcome::Installed,
                })
                .map_err(|e| e.to_string()),
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
            .await
            .map_err(map_error_to_napi_error);
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
                                .expect("Invalid response from 'get_state_callback'. Provide cancellation reason or new request. Currently both are empty.")
                                .into(),
                        )
                    }
                })
                .map_err(|e| e.to_string()),

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
