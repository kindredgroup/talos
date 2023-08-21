use crate::models::JsConfig;
use async_trait::async_trait;
use cohort_sdk::cohort_mock::CohortMock;
use cohort_sdk::model::callbacks::{CapturedItemState, CapturedState, ItemStateProvider, OutOfOrderInstallOutcome, OutOfOrderInstaller};
use cohort_sdk::model::{CandidateData, CertificationRequest};
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

impl From<JsCertificationRequest> for CertificationRequest {
    fn from(_val: JsCertificationRequest) -> Self {
        CertificationRequest {
            candidate: CandidateData {
                statemap: None,
                readset: vec![],
                writeset: vec![],
            },
            timeout_ms: 34,
        }
    }
}

#[napi]
pub struct Initiator {
    cohort: CohortMock,
}

#[napi]
impl Initiator {
    #[napi]
    pub async fn init(config: JsConfig) -> napi::Result<Initiator> {
        let cohort = CohortMock::create(config.into()).await.map_err(map_error_to_napi_error)?;
        // let tsfn: ThreadsafeFunction<OoRequest> = oo_callback
        //     .create_threadsafe_function(0, |ctx: ThreadSafeCallContext<OoRequest>| Ok(vec![ctx.value]))?;
        Ok(Initiator { cohort })
    }
    #[napi]
    pub async fn certify(
        &self,
        js_certification_request: JsCertificationRequest,
        get_state_callback: ThreadsafeFunction<u8>,
        ooo_callback: ThreadsafeFunction<OoRequest>,
    ) -> napi::Result<String> {
        println!("Initiator.certify()");
        let ooo_impl = OutOfOrderInstallerImpl { ooo_callback };
        let item_state_provider_impl = ItemStateProviderImpl {
            _get_state_callback: get_state_callback,
        };
        println!("Initiator.certify(): invoking cohort.certify(...)");
        let _res = self
            .cohort
            .certify(js_certification_request.into(), &item_state_provider_impl, &ooo_impl)
            .await
            .map_err(map_error_to_napi_error)?;

        println!("Initiator.certify(): after cohort.certify(...)");
        Ok("Success".to_string())
    }
}

struct OutOfOrderInstallerImpl {
    ooo_callback: ThreadsafeFunction<OoRequest>,
}

#[async_trait]
impl OutOfOrderInstaller for OutOfOrderInstallerImpl {
    async fn install(&self, xid: String, safepoint: u64, new_version: u64, attempt_nr: u32) -> Result<OutOfOrderInstallOutcome, String> {
        println!("OutOfOrderInstallerImpl.install()");
        let oorequest = OoRequest {
            xid,
            safepoint: safepoint.try_into().unwrap(),
            new_version: new_version.try_into().unwrap(),
            attempt_nr,
        };
        let result = self.ooo_callback.call_async::<()>(Ok(oorequest)).await.map_err(map_error_to_napi_error);
        match result {
            Ok(_) => {
                println!("OutOfOrderInstallerImpl.install(): was successful");
                Ok(OutOfOrderInstallOutcome::Installed)
            }
            Err(e) => {
                println!("OutOfOrderInstallerImpl.install(): error installing: {}", e);
                Err(e.to_string())
            }
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
    _get_state_callback: ThreadsafeFunction<u8>,
}

#[async_trait]
impl ItemStateProvider for ItemStateProviderImpl {
    async fn get_state(&self) -> Result<CapturedState, String> {
        Ok(CapturedState {
            snapshot_version: 34,
            items: vec![CapturedItemState {
                id: "sd".to_string(),
                version: 23,
            }],
        })
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

// impl From<JsClientError> for napi::Error {
//     fn from(client: JSScyllaError) -> Self {
//         map_error_to_napi_error(scylla_error)
//     }
// }
