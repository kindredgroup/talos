use crate::models::JsConfig;
use async_trait::async_trait;
use cohort_sdk::cohort::Cohort;
use cohort_sdk::model::callbacks::{CapturedItemState, CapturedState, ItemStateProvider, OutOfOrderInstallOutcome, OutOfOrderInstaller};
use cohort_sdk::model::{CandidateData, CertificationRequest};
use napi::bindgen_prelude::Promise;
use napi::threadsafe_function::ThreadsafeFunction;
use napi_derive::napi;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Display;

#[napi(object)]
pub struct JsCertificationRequest {
    pub candidate: JsCandidateData,
    pub timeout_ms: u32,
}

#[napi(object)]
pub struct JsCandidateData {
    pub readset: Vec<String>,
    pub writeset: Vec<String>,
    pub statemap: Option<Vec<HashMap<String, Value>>>,
}

impl From<JsCandidateData> for CandidateData {
    fn from(val: JsCandidateData) -> Self {
        CandidateData {
            readset: val.readset,
            writeset: val.writeset,
            statemap: val.statemap,
        }
    }
}

impl From<JsCertificationRequest> for CertificationRequest {
    fn from(val: JsCertificationRequest) -> Self {
        CertificationRequest {
            candidate: CandidateData::from(val.candidate),
            timeout_ms: val.timeout_ms as u64,
        }
    }
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
        js_certification_request: JsCertificationRequest,
        #[napi(ts_arg_type = "() => Promise<any>")] get_state_callback: ThreadsafeFunction<()>,
        ooo_callback: ThreadsafeFunction<OoRequest>,
    ) -> napi::Result<String> {
        // println!("Initiator.certify()");
        let ooo_impl = OutOfOrderInstallerImpl { ooo_callback };
        let item_state_provider_impl = ItemStateProviderImpl { get_state_callback };
        // println!("Initiator.certify(): invoking cohort.certify(...)");
        let _res = self
            .cohort
            .certify(js_certification_request.into(), &item_state_provider_impl, &ooo_impl)
            .await
            .map_err(map_error_to_napi_error)?;

        // println!("Initiator.certify(): after cohort.certify(...)");
        Ok("Success".to_string())
    }
}

struct OutOfOrderInstallerImpl {
    ooo_callback: ThreadsafeFunction<OoRequest>,
}

#[async_trait]
impl OutOfOrderInstaller for OutOfOrderInstallerImpl {
    async fn install(&self, xid: String, safepoint: u64, new_version: u64, attempt_nr: u32) -> Result<OutOfOrderInstallOutcome, String> {
        // println!("OutOfOrderInstallerImpl.install()");
        let oorequest = OoRequest {
            xid,
            safepoint: safepoint.try_into().unwrap(),
            new_version: new_version.try_into().unwrap(),
            attempt_nr,
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

// #[async_trait]
// impl OutOfOrderInstaller for Initiator {
//     async fn install(&self, xid: String, safepoint: u64, new_version: u64, attempt_nr: u64) -> Result<OutOfOrderInstallOutcome, String> {
//         println!("install being called");
//         let oorequest = OoRequest {
//             xid,
//             safepoint: safepoint.try_into().unwrap(),
//             new_version: new_version.try_into().unwrap(),
//             attempt_nr: attempt_nr.try_into().unwrap(),
//         };
//         let res: String = self.oo_callback.call_async( Ok(oorequest)).await.map_err(map_error_to_napi_error).unwrap();
//         println!("re from oo_callback {}, ", res);
//         Ok(OutOfOrderInstallOutcome::Installed)
//
//     }
// }

pub struct ItemStateProviderImpl {
    get_state_callback: ThreadsafeFunction<()>,
}

#[async_trait]
impl ItemStateProvider for ItemStateProviderImpl {
    async fn get_state(&self) -> Result<CapturedState, String> {
        let result = self
            .get_state_callback
            .call_async::<Promise<CapturedStateJs>>(Ok(()))
            .await
            .map_err(map_error_to_napi_error);
        match result {
            Ok(promise) => promise.await.map(CapturedState::from).map_err(|e| e.to_string()),
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
    pub attempt_nr: u32,
}

#[napi(object)]
pub struct CapturedItemStateJs {
    pub id: String,
    pub version: u32,
}

#[napi(object)]
pub struct CapturedStateJs {
    pub snapshot_version: u32,
    pub items: Vec<CapturedItemStateJs>,
}

impl From<CapturedStateJs> for CapturedState {
    fn from(val: CapturedStateJs) -> Self {
        Self {
            snapshot_version: val.snapshot_version as u64,
            items: val
                .items
                .iter()
                .map(|js| CapturedItemState {
                    id: js.id.clone(),
                    version: js.version as u64,
                })
                .collect(),
            abort_reason: None,
        }
    }
}

// impl From<JsClientError> for napi::Error {
//     fn from(client: JSScyllaError) -> Self {
//         map_error_to_napi_error(scylla_error)
//     }
// }
