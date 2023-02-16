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

#[derive(Debug, Clone)]
pub struct SuffixMeta {
    pub head: u64,
    /// The suffix version till where it's safe to prune.
    pub prune_vers: Option<u64>,
    /// The minimum size to be maintained by the suffix
    pub min_size: usize,
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
