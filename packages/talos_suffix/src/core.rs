use std::num::TryFromIntError;

use crate::errors::SuffixError;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SuffixItem<T> {
    /// Data received from Kafka Consumer
    pub item: SuffixItemType<T>,
    pub item_ver: u64,
    pub decision_ver: Option<u64>,
    /// Flag to denote if this item has been decided (is_decided is set to true irrespective of the outcome).
    pub is_decided: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SuffixConfig {
    /// Initial capacity of the suffix
    pub capacity: usize,
    /// Flag to enable/disable pruning.
    // pub enable_prune: bool,
    /// - The suffix prune threshold from when we start checking if the suffix
    /// should prune.
    /// - Set to None if pruning is not required.
    /// - Defaults to None.
    pub prune_start_threshold: Option<usize>,
    /// Minimum size of suffix after prune.
    /// Default value will be 30% of initial capacity.
    pub min_size_after_prune: Option<usize>,
}

impl Default for SuffixConfig {
    fn default() -> Self {
        const DEFAULT_CAPACITY: usize = 100_000;
        Self {
            capacity: DEFAULT_CAPACITY,
            prune_start_threshold: Default::default(),
            min_size_after_prune: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SuffixMeta {
    /// The version which is the head of the suffix
    pub head: u64,
    /// The last inserted version
    pub last_insert_vers: u64,
    /// The suffix index till where it's safe to prune.
    pub prune_index: Option<usize>,
    /// The suffix prune threshold from when we start checking if the suffix
    /// should prune.
    /// - If `None`, the suffix will not be pruned
    pub prune_start_threshold: Option<usize>,
    /// The minimum size to be maintained by the suffix.
    /// - If `None`, doesn't check for the minimum length to maintain in suffix while pruning.
    pub min_size_after_prune: Option<usize>,
}

type SuffixItemType<T> = T;

pub trait SuffixTrait<T> {
    fn get(&mut self, version: u64) -> SuffixResult<Option<SuffixItem<T>>>;
    fn insert(&mut self, version: u64, message: SuffixItemType<T>) -> SuffixResult<()>;
    fn update_decision(&mut self, version: u64, decision_ver: u64) -> SuffixResult<()>;
    fn prune(&mut self) -> SuffixResult<()>;
    fn remove(&mut self, version: u64) -> SuffixResult<()>;
}

pub fn convert_u64_to_usize(value: u64) -> Result<usize, TryFromIntError> {
    value.try_into()
}

// pub fn convert_usize_to_u64(value: usize) -> Result<u64, TryFromIntError> {
//     value.try_into()
// }

pub type SuffixResult<T> = Result<T, SuffixError>;
