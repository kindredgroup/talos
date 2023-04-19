// $coverage:ignore-start

pub fn expect_error<T>(response: Result<T, String>, error: &str) {
    let expected_error = if let Err(e) = response {
        if e.contains(error) {
            true
        } else {
            log::info!("Expected: '{}', got: '{}'", error, e);
            false
        }
    } else {
        false
    };
    assert!(expected_error);
}
// $coverage:ignore-end
