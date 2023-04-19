// $coverage:ignore-start
use std::fmt;
// $coverage:ignore-end
use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct TalosState {
    pub version: u64,
}

impl Display for TalosState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "TalosState: [version: {}]", self.version)
    }
}

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use crate::model::talos_state::TalosState;

    #[test]
    fn test() {
        assert_eq!(format!("{}", TalosState { version: 1 }.clone()), "TalosState: [version: 1]");
        assert_eq!(
            TalosState { version: 1 }.clone(),
            serde_json::from_str::<TalosState>("{ 'version': 1 }".to_string().replace('\'', "\"").as_str()).unwrap()
        );
        let _ = format!("{:?}", TalosState { version: 1 });
    }
}
// $coverage:ignore-end
