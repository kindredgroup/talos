use serde::Deserialize;
use std::fmt;
use std::fmt::{Display, Formatter};

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub struct AccountRef {
    pub number: String,
    pub new_version: Option<u64>,
}

impl AccountRef {
    pub fn new(number: String, new_version: Option<u64>) -> Self {
        Self { number, new_version }
    }
}

impl Display for AccountRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "AccountRef: [number: {}, new_version: {:?}]", self.number, self.new_version)
    }
}

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
        let acc_ref = AccountRef {
            number: "1".to_string(),
            new_version: None,
        };
        assert_eq!(
            acc_ref,
            AccountRef {
                number: "1".to_string(),
                new_version: None,
            },
        );
        assert_ne!(
            acc_ref,
            AccountRef {
                number: "1".to_string(),
                new_version: Some(1),
            },
        );
        assert_eq!(format!("{}", acc_ref), "AccountRef: [number: 1, new_version: None]".to_string());

        assert_eq!(Snapshot { version: 1 }, Snapshot::from(1));
        assert_ne!(Snapshot { version: 2 }, Snapshot::from(1));

        assert_eq!(format!("{}", Snapshot::from(1)), "Snapshot: [version: 1]".to_string());
    }

    #[test]
    fn should_deserialize_snapshot() {
        let rslt_snapshot = serde_json::from_str::<Snapshot>("{ \"version\": 123456 }");
        let snapshot = rslt_snapshot.unwrap();
        assert_eq!(snapshot.version, 123456);
    }
}
// $coverage:ignore-end
