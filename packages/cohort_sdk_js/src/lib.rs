use std::fmt::Display;

pub mod initiator;
pub mod installer;
pub mod models;
pub mod sdk_errors;

fn map_error_to_napi_error<T: Display>(e: T) -> napi::Error {
    napi::Error::from_reason(e.to_string())
}
