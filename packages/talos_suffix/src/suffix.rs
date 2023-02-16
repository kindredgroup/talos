// Suffix

use std::collections::VecDeque;

use log::{debug, info};

use crate::{
    core::{convert_u64_to_usize, SuffixMeta, SuffixResult, SuffixTrait},
    errors::SuffixError,
    SuffixItem,
};

#[derive(Debug, Clone)]
pub struct Suffix<T> {
    pub meta: SuffixMeta,
    pub messages: VecDeque<Option<SuffixItem<T>>>,
}

impl<T> Suffix<T>
where
    T: Sized + Clone + std::fmt::Debug,
{
    pub fn new(min_size: usize, capacity: usize) -> Suffix<T> {
        // let suffix_vec: Vec<Option<SuffixItem<T>>> = vec![None; capacity];
        let messages = VecDeque::with_capacity(capacity);

        let meta = SuffixMeta {
            head: 0,
            prune_vers: None,
            min_size,
        };

        Suffix { meta, messages }
    }

    pub fn index_from_head(&self, version: u64) -> Option<usize> {
        let head = self.meta.head;
        if version < head {
            None
        } else {
            Some(convert_u64_to_usize(version - head).ok()?)
        }
    }

    fn update_head(&mut self, version: u64) {
        self.meta.head = version;
    }

    pub fn update_prune_vers(&mut self, vers: Option<u64>) {
        self.meta.prune_vers = vers;
    }

    /// Reserve space when the version we are inserting is
    /// outside the upper bounds of suffix.
    ///
    /// Reserves space and defaults them with None.
    pub fn reserve_space_if_required(&mut self, version: u64) -> Result<(), SuffixError> {
        let ver_diff: usize = (version - self.meta.head) as usize + 1;

        if ver_diff.gt(&(self.messages.len())) {
            // let resize_len = if ver_diff < self.meta.min_size { self.meta.min_size + 1 } else { ver_diff };
            self.messages.reserve(ver_diff + 1);
        }

        Ok(())
    }

    /// Find prior versions are all decided.
    ///
    /// Returns true, if the versions prior to the current version has either been decided or
    /// if suffix item is empty (None).
    pub fn are_prior_items_decided(&mut self, version: u64) -> bool {
        let mut result = false;
        let Some(index) = self.index_from_head(version) else {
            return result;
        };

        let Some(prune_vers) = self.meta.prune_vers else {
            return true;
        };

        let prune_index = self.index_from_head(prune_vers).unwrap_or(0);

        if index > prune_index {
            result = self
                .messages
                .range(prune_index..index)
                .filter_map(|x| x.is_some().then(|| x.as_ref().unwrap()))
                .all(|x| x.is_decided);
        } else {
            info!(
                "Split index used is {index} for version {version} and message length is {}",
                self.messages.len()
            );
            let slice = self.messages.make_contiguous().split_at(index).0;
            result = slice.iter().filter_map(|x| x.is_some().then(|| x.as_ref().unwrap())).all(|x| x.is_decided);
        }

        result
    }

    /// Find the next valid suffix item from a particular version.
    /// Returns None if there are no valid suffix item
    fn find_next_message(&self, from_version: u64) -> Option<SuffixItem<T>> {
        self.messages
            .iter()
            .find(|&x| x.is_some() && x.clone().unwrap().item_ver > from_version)?
            .clone()
    }

    pub fn update_decision_suffix_item(&mut self, version: u64, decision_ver: u64) -> SuffixResult<()> {
        // When Certifier is catching up with messages ignore the messages which are prior to the head
        if version < self.meta.head {
            info!("Returned due to version < self.meta.head for version={version} and decision version={decision_ver}");
            return Ok(());
        }

        let Some(sfx_item) = self
        .get(version)?
        else {
                info!("Returned due item not found in suffix for version={version} and decision version={decision_ver}");
                return Ok(());
            };

        let new_sfx_item = SuffixItem {
            decision_ver: Some(decision_ver),
            is_decided: true,
            ..sfx_item
        };

        let index = self
            .index_from_head(version)
            .ok_or(SuffixError::IndexCalculationError(self.meta.head, version))?;

        info!("Updating version={version} with index={index:?} and decision version={decision_ver}");
        self.messages[index] = Some(new_sfx_item);
        Ok(())
    }
}

impl<T> SuffixTrait<T> for Suffix<T>
where
    T: Sized + Clone + std::fmt::Debug,
{
    fn get(&mut self, version: u64) -> SuffixResult<Option<SuffixItem<T>>> {
        let index = self.index_from_head(version).ok_or(SuffixError::VersionToIndexConversionError(version))?;
        let suffix_item = self.messages.get(index).and_then(|x| x.as_ref()).cloned();
        info!("[SUFFIX GET] ver={version} index={index}");

        Ok(suffix_item)
    }

    fn insert(&mut self, version: u64, message: T) -> SuffixResult<()> {
        // // The very first item inserted on the suffix will automatically be made head of the suffix.
        if self.meta.head == 0 {
            self.update_head(version);
        }

        if self.meta.head.le(&version) {
            self.reserve_space_if_required(version)?;
            let index = self.index_from_head(version).ok_or(SuffixError::ItemNotFound(version, None))?;
            info!(
                "GK - going to insert to suffix with len={}, HEAD={}, version={version} and index={index}",
                self.messages.len(),
                self.meta.head
            );

            let none_item_block = self.messages.push_back(Some(SuffixItem {
                item: message,
                item_ver: version,
                decision_ver: None,
                is_decided: false,
            }));

            let k: Vec<(usize, u64, Option<u64>)> = self
                .messages
                .iter()
                .enumerate()
                .filter_map(|(i, x)| x.is_some().then(|| (i, x.as_ref().unwrap().item_ver, x.as_ref().unwrap().decision_ver)))
                .collect();
            info!("[SUFFIX INSERT] Suffix dump \n{k:?}");
        }

        Ok(())
    }

    fn update_decision(&mut self, version: u64, decision_ver: u64) -> SuffixResult<()> {
        // When Certifier is catching up with messages ignore the messages which are prior to the head
        if version < self.meta.head {
            return Ok(());
        }

        self.update_decision_suffix_item(version, decision_ver)?;

        if self.are_prior_items_decided(version) {
            self.update_prune_vers(Some(version));
        }

        Ok(())
    }

    /// Prune the suffix
    ///
    /// Looks at the meta to find the prune ready version.
    ///
    /// Prune is allowed when
    ///     1. The meta has a valid prune version.
    ///     2. And there is atleast one suffix item remaining, which can be the new head.
    ///        This enables to move the head to the appropiate location.
    fn prune(&mut self) -> SuffixResult<()> {
        let ver = self.meta.prune_vers.ok_or(SuffixError::PruneVersionNotFound)?;

        if let Some(suffix_item) = self.find_next_message(ver) {
            info!("Next suffix item found is.....{:?}", suffix_item.item_ver);

            let k: Vec<u64> = self.messages.iter().filter_map(|x| x.is_some().then(|| x.as_ref().unwrap().item_ver)).collect();
            info!("Items before pruning are \n{k:?}");

            let next_index = self
                .index_from_head(suffix_item.item_ver)
                .ok_or(SuffixError::VersionToIndexConversionError(ver))?;
            drop(self.messages.drain(0..(self.meta.min_size)));

            self.update_prune_vers(None);

            self.update_head(suffix_item.item_ver);

            let k: Vec<(u64, Option<u64>)> = self
                .messages
                .iter()
                .filter_map(|x| x.is_some().then(|| (x.as_ref().unwrap().item_ver, x.as_ref().unwrap().decision_ver)))
                .collect();
            info!("Items after pruning are \n{k:?}");
        }

        Ok(())
    }

    fn remove(&mut self, version: u64) -> SuffixResult<()> {
        let index = self.index_from_head(version).ok_or(SuffixError::ItemNotFound(version, None))?;

        self.messages[index] = None;

        // if we are removing the pruneable version, set prune_vers to None.
        if self.meta.prune_vers == Some(version) {
            self.update_prune_vers(None);
        }

        Ok(())
    }
}
