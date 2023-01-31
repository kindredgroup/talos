use ahash::AHashMap;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CertifierCandidate {
    pub vers: u64,
    pub snapshot: u64,
    pub readvers: Vec<u64>,
    pub readset: Vec<String>,
    pub writeset: Vec<String>,
}

impl CertifierCandidate {
    pub fn is_version_above_snapshot(&self, version: u64) -> bool {
        version > self.snapshot
    }

    pub fn convert_readset_to_collection(&self) -> AHashMap<String, u64> {
        self.readset.clone().into_iter().map(|k| (k, self.vers)).collect()
    }
    pub fn convert_writeset_to_collection(&self) -> AHashMap<String, u64> {
        self.writeset.clone().into_iter().map(|k| (k, self.vers)).collect()
    }
}
