use crate::models::JsConfig;
use async_trait::async_trait;
use cohort_sdk::cohort::Cohort;
use cohort_sdk::model::callback::{
    CertificationCandidate, CertificationCandidateCallbackResponse, CertificationRequestPayload, OutOfOrderInstallOutcome, OutOfOrderInstallRequest,
    OutOfOrderInstaller,
};
use napi::bindgen_prelude::Promise;
use napi::threadsafe_function::ThreadsafeFunction;
use napi_derive::napi;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Display;

#[napi(object)]
pub struct JsCertificationCandidate {
    pub readset: Vec<String>,
    pub writeset: Vec<String>,
    pub readvers: Vec<u32>,
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
    pub snapshot: u32,
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
    pub async fn init(config: JsConfig) -> napi::Result<Initiator> {
        let cohort = Cohort::create(config.into()).await.map_err(map_error_to_napi_error)?;
        Ok(Initiator { cohort })
    }

    #[napi]
    pub async fn certify(
        &self,
        #[napi(ts_arg_type = "() => Promise<any>")] get_state_callback: ThreadsafeFunction<()>,
        ooo_callback: ThreadsafeFunction<OoRequest>,
    ) -> napi::Result<String> {
        // println!("Initiator.certify()");
        let item_state_provider_impl = ItemStateProviderImpl { get_state_callback };
        let ooo_impl = OutOfOrderInstallerImpl { ooo_callback };
        // println!("Initiator.certify(): invoking cohort.certify(...)");
        let make_new_request = || item_state_provider_impl.get_state();
        let _res = self.cohort.certify(&make_new_request, &ooo_impl).await.map_err(map_error_to_napi_error)?;

        // println!("Initiator.certify(): after cohort.certify(...)");
        Ok("Success".to_string())
    }
}

struct OutOfOrderInstallerImpl {
    ooo_callback: ThreadsafeFunction<OoRequest>,
}

#[async_trait]
impl OutOfOrderInstaller for OutOfOrderInstallerImpl {
    async fn install(&self, request: OutOfOrderInstallRequest) -> Result<OutOfOrderInstallOutcome, String> {
        let oorequest = OoRequest {
            xid: request.xid,
            safepoint: request.safepoint.try_into().unwrap(),
            new_version: request.version.try_into().unwrap(),
        };

        let result = self
            .ooo_callback
            .call_async::<Promise<u32>>(Ok(oorequest))
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

pub struct ItemStateProviderImpl {
    get_state_callback: ThreadsafeFunction<()>,
}

impl ItemStateProviderImpl {
    async fn get_state(&self) -> Result<CertificationCandidateCallbackResponse, String> {
        let result = self
            .get_state_callback
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

// pub struct JsClientError {
//     client_error: ClientError
// }
/// # Errors
/// Convert rust error into `napi::Error`
fn map_error_to_napi_error<T: Display>(e: T) -> napi::Error {
    napi::Error::from_reason(e.to_string())
}
#[napi(object)]
pub struct OoRequest {
    pub xid: String,
    pub safepoint: u32,
    pub new_version: u32,
}

// impl From<JsClientError> for napi::Error {
//     fn from(client: JSScyllaError) -> Self {
//         map_error_to_napi_error(scylla_error)
//     }
// }
