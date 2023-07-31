use std::fmt;
use std::fmt::{Display, Formatter};

use serde::Deserialize;

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Deserialize)]
pub struct Snapshot {
    pub version: u64,
}

impl Snapshot {
    pub fn is_safe_for(snapshot: Snapshot, safepoint: u64) -> bool {
        snapshot.version >= safepoint
    }
}

impl Display for Snapshot {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Snapshot: [version: {:?}]", self.version)
    }
}

impl From<u64> for Snapshot {
    fn from(value: u64) -> Self {
        Snapshot { version: value }
    }
}

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn models() {
        assert_eq!(Snapshot { version: 1 }, Snapshot::from(1));
        assert_ne!(Snapshot { version: 2 }, Snapshot::from(1));
    }

    #[test]
    fn should_deserialize_snapshot() {
        let rslt_snapshot = serde_json::from_str::<Snapshot>(r#"{ "version": 123456 }"#);
        let snapshot = rslt_snapshot.unwrap();
        assert_eq!(snapshot.version, 123456);
    }

    #[test]
    #[allow(clippy::bool_assert_comparison)]
    fn when_snapshot_is_behind_safepoint_ist_not_safe() {
        let safepoint = 2;
        assert_eq!(Snapshot::is_safe_for(Snapshot { version: 1 }, safepoint), false);
        assert_eq!(Snapshot::is_safe_for(Snapshot { version: 2 }, safepoint), true);
        assert_eq!(Snapshot::is_safe_for(Snapshot { version: 3 }, safepoint), true);
    }
}
// $coverage:ignore-end
