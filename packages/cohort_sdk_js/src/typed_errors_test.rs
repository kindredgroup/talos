use napi_derive::napi;

use crate::sdk_errors::{SdkErrorContainer, SdkErrorKind};

#[napi]
pub struct InternalSomeRustServiceClass {}

#[napi]
impl InternalSomeRustServiceClass {
    #[napi]
    pub fn example1_direct_throw(&self, kind: SdkErrorKind) -> napi::Result<()> {
        Err(napi::Error::from_reason(
            SdkErrorContainer::new(kind, "This error is encoded as JSON and transported in 'napi::Error.reason'".into(), None)
                .json()
                .to_string(),
        ))
    }

    // #[napi]
    // pub fn example2_unexpected_rust_error(&self) -> napi::Result<()> {
    //     let no_data: Option<String> = None;
    //     no_data.unwrap();
    //     Ok(())
    // }
}
