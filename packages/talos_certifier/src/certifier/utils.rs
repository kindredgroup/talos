use ahash::{AHashMap, HashMap, HashMapExt};
use talos_suffix::SuffixItem;

use crate::model::CandidateReadWriteSet;

use super::{CertifierReadset, CertifierWriteset};

pub fn convert_vec_to_hashmap(v: Vec<(&str, u64)>) -> AHashMap<String, u64> {
    v.into_iter().map(|(k, v)| (k.to_owned(), v)).collect()
}

/// Generates readset and writeset for the certifier for the suffix items provided.
///
/// ## Note:
/// As the readset and writesets are hashmaps, if there are duplicate inserts of readset/writeset for different suffix versions,
/// the last version will be stored in the value. Therefore ensure the vector is sorted to prevent incorrect
/// read and write sets.
pub fn generate_certifier_sets_from_suffix<'a, T>(items: impl Iterator<Item = &'a SuffixItem<T>>) -> (CertifierReadset, CertifierWriteset)
where
    T: 'a + CandidateReadWriteSet,
{
    items.fold(
        (HashMap::<String, u64>::new().into(), HashMap::<String, u64>::new().into()),
        |mut acc, suffix_item| {
            suffix_item.item.get_readset().iter().for_each(|read_item| {
                acc.0.insert(read_item.to_string(), suffix_item.item_ver);
            });
            suffix_item.item.get_writeset().iter().for_each(|write_item| {
                acc.1.insert(write_item.to_string(), suffix_item.item_ver);
            });
            acc
        },
    )
}
