use std::fmt;
use std::fmt::{Display, Formatter};

use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct TalosState {
    pub version: u64,
}

impl Display for TalosState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "TalosState: [version: {}]", self.version)
    }
}
