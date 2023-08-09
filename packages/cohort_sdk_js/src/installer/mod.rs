use std::fmt::Display;
use cohort_sdk::model::callbacks::StatemapInstaller;
use cohort_sdk::replicator::core::StatemapItem;
use async_trait::async_trait;
use napi::{CallContext, JsFunction, JsUndefined};
use crate::models::JsConfig;
use napi::bindgen_prelude::*;
use napi::threadsafe_function::ErrorStrategy::ErrorStrategy;
use napi::threadsafe_function::ThreadsafeFunction;
use napi_derive::napi;
use cohort_sdk::cohort::Cohort;
use cohort_sdk::model::Config;
/*
Replicator needs constructor to pass config and impl StatemapInstaller.
Replicator needs init method to start the background process of replicator.
On Installation, Replicator calls install method of StatemapInstallerImpl. Which in turn should pass back the control to JsWorld.

*/

struct MockRustCohort {}
#[napi]
pub struct Replicator {
    cohort: Cohort,
}
#[napi]
impl Replicator {
    #[napi]
    pub fn init(
        config: JsConfig,
        #[napi(ts_arg_type = "callback: (err: null | Error, result: number) => void")]
        install_callback: JsFunction
    ) ->napi::Result<Replicator> {

        Ok(Replicator{
            cohort: Cohort::create(
                config,
                StatemapInstallerImpl {
                    install_callback
                }
            ).expect("error while initiating cohort")
        });
        get_thread_safe_function();
        // let js_installer = StatemapInstallerImpl {
        //     config: config,
        //     install: install_callback
        // };
        // let cohort = Cohort::create(
        //     config.into(),
        //     js_installer
        // ).await?;
        // Ok(Replicator {cohort})
    }

    #[napi]
    pub async fn start() -> napi::Result<()> {
        Ok(())
    }
}




// #[derive(Debug, thiserror::Error)]
// pub enum JSReplicatorError {
//     #[error("Validation failed for fields: {0}")]
//     ArgumentValidationError(String),
//     // #[error("Error: {0}")]
//     // DomainError(#[from] PgAdapterError),
// }

/// # Errors
/// Convert rust error into `napi::Error`
// fn map_error_to_napi_error<T: Display>(e: T) -> napi::Error {
//     napi::Error::from_reason(e.to_string())
// }
//
// impl From<JSReplicatorError> for napi::Error {
//     fn from(scylla_error: JSReplicatorError) -> Self {
//         map_error_to_napi_error(scylla_error)
//     }
// }
// macro_rules! map_lib_response {
//     ($task_result: expr) => {
//         match $task_result {
//             Ok(t) => Ok(serde_json::to_string(&t).unwrap()),
//             Err(e) => Err(napi::Error::from_reason(e.to_string())),
//         }
//     };
// }

// #[napi]
// pub fn init(callback: JsFunction) {
//     let tsfn: ThreadsafeFunction<Result<()>, > = callback
//         .create_threadsafe_function(0, |ctx| {
//             ctx.env.create_uint32(ctx.value + 1).map(|v| vec![v])
//         })?;
// }
//
//
//
//
pub struct StatemapInstallerImpl {
    pub install_callback: ThreadsafeFunction<JsUndefined>
}

impl StatemapInstallerImpl {
   pub fn init(callback: JsFunction) {

   }
}


#[async_trait]
impl StatemapInstaller for StatemapInstallerImpl {
    async fn install(&self, statemap: Vec<StatemapItem>, snapshot_version: u64) -> core::result::Result<(), String> {
      self.install_callback().await?;
        Ok(())
    }
}
//
// use std::thread;
//
// // use napi_derive::napi;
// use napi::{bindgen_prelude::*, JsObject, NapiValue, threadsafe_function::{ErrorStrategy, ThreadsafeFunction, ThreadsafeFunctionCallMode}, tokio};
//
//
//
// #[napi]
// pub fn call_threadsafe_function(callback: JsFunction) -> Result<()> {
//     let tsfn: ThreadsafeFunction<(Vec<u32>, Box<dyn Fn(JsObject)>), ErrorStrategy::CalleeHandled> = callback
//         .create_threadsafe_function(0, |ctx:ThreadSafeCallContext<(Vec<u32>, Box<dyn Fn(JsObject)>)>| {
//             println!("context is here: {:?}", ctx.value.0);
//             let args = ctx.value.0
//                 .iter()
//                 .map(|v| ctx.env.to_js_value(v))
//                 .collect::<Result<Vec<JsNumber>, napi::Error>>()?;
//             let return_callback = unsafe {JsFunction::from_raw_unchecked(ctx.env, ctx.value.1)};
//             return (args, return_callback);
//         })?;
//     let tsfn_cloned = tsfn.clone();
//
//     // thread::spawn(move || {
//     //     let output: Vec<u32> = vec![0, 1, 2, 3];
//     //     // It's okay to call a threadsafe function multiple times.
//     //     tsfn.call(Ok(output.clone()), ThreadsafeFunctionCallMode::Blocking);
//     // });
//
//     tokio::spawn(async move {
//         let output: Vec<u32> = vec![3, 2, 1, 0];
//         // It's okay to call a threadsafe function multiple times.
//         let f = ThreadsafeFunction::<Vec<u32>>::call_async::<JsNumber>(&tsfn_cloned, Ok(output.clone()));
//         let s = f.await.unwrap();
//         // println!("s is : {:?}", s);
//     });
//     println!("my job is done");
//     Ok(())
// }
//
//
//
// // src/lib.rs
//
//
//
//
// use napi::{
//     threadsafe_function::{
//         ThreadSafeCallContext,
//     },
//     CallContext, Error, JsFunction, JsNumber, JsUndefined, Result, Status,
// };
//
//
