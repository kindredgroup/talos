use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use async_trait::async_trait;
use napi_derive::napi;
use napi::{Error,  JsFunction, JsObject, JsString};
use napi::bindgen_prelude::{BigInt, Promise};
use napi::threadsafe_function::ErrorStrategy::ErrorStrategy;
use napi::threadsafe_function::{ThreadSafeCallContext, ThreadsafeFunction};
use serde_json::Value;
use cohort_sdk::cohort::Cohort;
use cohort_sdk::model::{CandidateData, CertificationRequest, ClientError, ClientErrorKind};
use cohort_sdk::model::callbacks::{CapturedItemState, CapturedState, ItemStateProvider, OutOfOrderInstaller, OutOfOrderInstallOutcome};
use crate::models::JsConfig;


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

impl Into<CertificationRequest> for JsCertificationRequest {
    fn into(self) -> CertificationRequest {
        CertificationRequest {
            candidate: CandidateData {
                statemap: None,
                readset: vec![],
                writeset: vec![]
            },
            timeout_ms: 34
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
    pub async fn init(config:JsConfig) -> napi::Result<Initiator> {

        let cohort = Cohort::create(config.into()).await.map_err(map_error_to_napi_error)?;
        // let tsfn: ThreadsafeFunction<OORequest> = oo_callback
        //     .create_threadsafe_function(0, |ctx: ThreadSafeCallContext<OORequest>| Ok(vec![ctx.value]))?;
        Ok(Initiator {
            cohort,
        })

    }
    #[napi]
    pub async fn certify(&self, js_certification_request: JsCertificationRequest, ooo_callback: ThreadsafeFunction<OORequest>, get_state_callback: ThreadsafeFunction<u8>) -> napi::Result<String>{
        println!("certify being called");
        let ooo_impl = OutOfOrderInstallerImpl{ooo_callback};
        let item_state_provider_impl = ItemStateProviderImpl{get_state_callback};
        let res = self.cohort.certify(js_certification_request.into(), &item_state_provider_impl, &ooo_impl).await.map_err(map_error_to_napi_error)?;
        Ok("Success".to_string())
    }
}

struct OutOfOrderInstallerImpl{
    ooo_callback: ThreadsafeFunction<OORequest>,
}

#[async_trait]
impl OutOfOrderInstaller for OutOfOrderInstallerImpl {
    async fn install(&self, xid: String, safepoint: u64, new_version: u64, attempt_nr: u64) -> Result<OutOfOrderInstallOutcome, String> {
        println!("install being called");
        let oorequest = OORequest {
            xid,
            safepoint: safepoint.try_into().unwrap(),
            new_version: new_version.try_into().unwrap(),
            attempt_nr: attempt_nr.try_into().unwrap(),
        };
        let res: String = self.ooo_callback.call_async( Ok(oorequest)).await.map_err(map_error_to_napi_error).unwrap();
        println!("re from oo_callback {}, ", res);
        Ok(OutOfOrderInstallOutcome::Installed)
    }

}

// #[async_trait]
// impl OutOfOrderInstaller for Initiator {
//     async fn install(&self, xid: String, safepoint: u64, new_version: u64, attempt_nr: u64) -> Result<OutOfOrderInstallOutcome, String> {
//         println!("install being called");
//         let oorequest = OORequest {
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
    get_state_callback: ThreadsafeFunction<u8>
}
#[async_trait]
impl ItemStateProvider for ItemStateProviderImpl {
    async fn get_state(&self) -> Result<CapturedState, String> {
        Ok(CapturedState {
            snapshot_version: 34,
            items: vec![CapturedItemState {
                id: "sd".to_string(),
                version: 23
            }]
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
pub struct OORequest {
    pub xid: String,
    pub safepoint: u32,
    pub new_version: u32,
    pub attempt_nr: u32
}

// impl From<JsClientError> for napi::Error {
//     fn from(client: JSScyllaError) -> Self {
//         map_error_to_napi_error(scylla_error)
//     }
// }

