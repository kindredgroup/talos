// Suffix

use std::collections::VecDeque;

use log::{debug, info, warn};

use crate::{
    core::{SuffixConfig, SuffixMeta, SuffixResult, SuffixTrait},
    errors::SuffixError,
    utils::get_nonempty_suffix_items,
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
    /// Creates a new suffix using the config passed.
    ///
    /// The config can be used to control
    /// - Required:
    ///     - `capacity` - The initial capacity of the suffix.
    /// - Optional:
    ///     - `prune_start_threshold` - The threshold index beyond which pruning starts.
    ///         - If `None`, no pruning will occur.
    ///         - If `Some()`, attempts to prune suffix if suffix length crosses the threshold.
    ///
    pub fn with_config(config: SuffixConfig) -> Suffix<T> {
        let SuffixConfig {
            capacity,
            prune_start_threshold,
            min_size_after_prune,
        } = config;

        let messages = VecDeque::with_capacity(capacity);

        assert!(
            min_size_after_prune <= prune_start_threshold,
            "The config min_size_after={:?} is greater than prune_start_threshold={:?}",
            min_size_after_prune,
            prune_start_threshold
        );

        let meta = SuffixMeta {
            head: 0,
            last_insert_vers: 0,
            prune_index: None,
            prune_start_threshold,
            min_size_after_prune,
        };

        Suffix { meta, messages }
    }

    pub fn index_from_head(&self, version: u64) -> Option<usize> {
        let head = self.meta.head;
        if version < head {
            None
        } else {
            Some((version - head) as usize)
        }
    }

    pub fn suffix_length(&self) -> usize {
        self.messages.len()
    }

    fn is_valid_prune_version_index(&self) -> bool {
        // If none, not valid
        let Some(prune_index) = self.meta.prune_index else {
            return false;
        };

        // If no greater than 0, not valid
        prune_index > 0
    }

    fn update_head(&mut self, version: u64) {
        self.meta.head = version;
    }

    pub fn update_prune_index(&mut self, index: Option<usize>) {
        self.meta.prune_index = index;
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
        let Some(index) = self.index_from_head(version) else {
            return false;
        };

        // If prune index is `None` assumption is this is the first item.
        let prune_index = self.meta.prune_index.unwrap_or(0);

        let range = if index > prune_index { prune_index..index } else { 0..index };

        get_nonempty_suffix_items(self.messages.range(range)).all(|k| k.is_decided)
    }

    pub fn retrieve_all_some_vec_items(&self) -> Vec<(usize, u64, Option<u64>)> {
        self.messages
            .iter()
            .enumerate()
            .filter_map(|(i, x)| x.is_some().then(|| (i, x.as_ref().unwrap().item_ver, x.as_ref().unwrap().decision_ver)))
            .collect()
    }

    pub fn find_prune_till_index(&mut self, prune_till_index: usize) -> usize {
        let prune_till_index = self
            .messages
            .range(..prune_till_index + 1)
            .enumerate()
            .rev()
            .find_map(|(i, x)| x.is_some().then_some(i))
            .unwrap();

        prune_till_index
    }

    pub fn get_safe_prune_index(&mut self) -> Option<usize> {
        // If `prune_start_threshold=None` don't prune.
        let Some(prune_threshold) = self.meta.prune_start_threshold else {
            debug!(
                "[SUFFIX PRUNE CHECK] As suffix.meta.prune_start_threshold is None, pruning is disabled."
            );
            return None;
        };

        // If not reached the max threshold
        if self.suffix_length() < prune_threshold {
            debug!(
                "[is_ready_for_prune] returning None because suffix.len={} < {prune_threshold}",
                self.suffix_length()
            );
            return None;
        }

        // Not ready to prune, if prune version is not set
        if !self.is_valid_prune_version_index() {
            debug!("[SUFFIX PRUNE CHECK] suffix.meta.prune_index is None, not ready to prune at the moment.");
            return None;
        }

        let mut prune_index = self.meta.prune_index;

        // If the `min_size_after_prune=None`, then we prune and the current prune index.
        let Some(suffix_min_size) = self.meta.min_size_after_prune else {
            return prune_index;
        };

        let min_threshold_index = self.messages.len() - suffix_min_size - 1;

        if min_threshold_index <= prune_index.unwrap() {
            if self.messages[min_threshold_index].is_some() {
                prune_index = Some(min_threshold_index);
            } else {
                let next_prune_index = self.find_prune_till_index(min_threshold_index);
                prune_index = Some(next_prune_index);
            }
            return prune_index;
        }

        None
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
                info!("Returned due item not found in suffix for version={version} with index={:?}  and decision version={decision_ver}", self.index_from_head(version));
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

        debug!("Updating version={version} with index={index:?} and decision version={decision_ver}");
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

            if index > 0 {
                let last_item_index = self.index_from_head(self.meta.last_insert_vers).unwrap_or(0);
                for _ in (last_item_index + 1)..index {
                    self.messages.push_back(None);
                }
            }

            self.messages.push_back(Some(SuffixItem {
                item: message,
                item_ver: version,
                decision_ver: None,
                is_decided: false,
            }));

            self.meta.last_insert_vers = version;
        }

        Ok(())
    }

    fn update_decision(&mut self, version: u64, decision_ver: u64) -> SuffixResult<()> {
        // When Certifier is catching up with messages ignore the messages which are prior to the head
        if version < self.meta.head {
            warn!("Version {version} is below the suffix head {}. Skipping updates", self.meta.head);
            return Ok(());
        }

        self.update_decision_suffix_item(version, decision_ver)?;

        if self.are_prior_items_decided(version) {
            let index = self.index_from_head(version).unwrap();
            self.update_prune_index(Some(index));
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
    fn prune_till_index(&mut self, index: usize) -> SuffixResult<Vec<Option<SuffixItem<T>>>> {
        info!("Suffix message length BEFORE pruning={} and head={}!!!", self.messages.len(), self.meta.head);
        // info!("Next suffix item index= {:?} after prune index={prune_index:?}.....", suffix_item.item_ver);

        // let k = self.retrieve_all_some_vec_items();
        // info!("Items before pruning are \n{k:?}");

        let drained_entries = self.messages.drain(..=index).collect();

        self.update_prune_index(None);

        if let Some(Some(s_item)) = self.messages.iter().find(|m| m.is_some()) {
            self.update_head(s_item.item_ver);
        } else {
            self.update_head(0)
        }

        // info!("Suffix message length AFTER pruning={} and head={}!!!", self.messages.len(), self.meta.head);
        // let k = self.retrieve_all_some_vec_items();
        // info!("Items after pruning are \n{k:?}");
        // }

        Ok(drained_entries)
    }

    fn prune_till_version(&mut self, version: u64) -> SuffixResult<Vec<Option<SuffixItem<T>>>> {
        info!("Suffix before prune.... {}", self.suffix_length());
        if let Some(index) = self.index_from_head(version) {
            info!("Index send for pruning is {index} for version={version}");
            let prune_result = self.prune_till_index(index);
            info!("Suffix items pruned.... {prune_result:?}");
            info!("Suffix after prune.... {}", self.suffix_length());
            info!("Items on suffix after pruning = {:#?}", self.retrieve_all_some_vec_items());
            return prune_result;
        } else {
            warn!("Unable to prune as index not found for version {version}.")
        }
        Ok(vec![])
    }

    fn remove(&mut self, version: u64) -> SuffixResult<()> {
        let index = self.index_from_head(version).ok_or(SuffixError::ItemNotFound(version, None))?;

        self.messages[index] = None;

        // if we are removing the pruneable version, set prune_vers to None.
        if self.meta.prune_index == Some(index) {
            self.update_prune_index(None);
        }

        Ok(())
    }
}
