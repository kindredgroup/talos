// Suffix

use std::{collections::VecDeque, time::Instant};

use opentelemetry::{global, metrics::Gauge};
use tracing::{debug, error, info, warn};

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
    pub g_suffix_head: Option<Gauge<u64>>,
    pub g_suffix_length: Option<Gauge<u64>>,
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

        let meter = global::meter("suffix");
        Suffix {
            meta,
            messages,
            g_suffix_length: Some(meter.u64_gauge("suffix_length").build()),
            g_suffix_head: Some(meter.u64_gauge("suffix_head").build()),
        }
    }

    /// Resets the suffix to initial state.
    pub fn reset(&mut self) {
        // Reset suffix meta.
        self.meta.head = 0;
        self.meta.last_insert_vers = 0;
        self.meta.prune_index = None;

        // Clear the suffix messages vecdequeue.
        self.messages.clear();
        self.update_metrics();
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
            .flatten()
            .enumerate()
            .map(|(i, x)| (i, x.item_ver, x.decision_ver))
            .collect()
    }

    pub fn find_prune_till_index(&self, prune_till_index: usize) -> usize {
        let prune_till_index = self
            .messages
            .range(..prune_till_index + 1)
            .enumerate()
            .rev()
            .find_map(|(i, x)| x.is_some().then_some(i))
            .unwrap();

        prune_till_index
    }

    pub fn get_safe_prune_index(&self) -> Option<usize> {
        // If `prune_start_threshold=None` don't prune.
        let Some(prune_threshold) = self.meta.prune_start_threshold else {
            debug!("[SUFFIX PRUNE CHECK] As suffix.meta.prune_start_threshold is None, pruning is disabled.");
            return None;
        };

        // If prune_index is less than prune_threshold, it is not ready
        if let Some(prune_index) = self.meta.prune_index {
            if prune_index.lt(&prune_threshold) {
                debug!("[SUFFIX PRUNE CHECK] Prune index {prune_index} is less than prune_threshold {prune_threshold}. Not ready to prune yet.");
                return None;
            }
        }

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
            warn!(
                "Returned due to version < suffix.head ({}) for version={version} and decision version={decision_ver}",
                self.meta.head
            );
            return Ok(());
        }

        let Some(sfx_item) = self.get(version)? else {
            warn!(
                "Returned due item not found in suffix for version={version} with index={:?}  and decision version={decision_ver}. suffix.head = {}",
                self.index_from_head(version),
                self.meta.head
            );

            // This search would be expensive, but this is executed only when version is not found and this helps in debugging when the suffix is inserted in incorrect index.
            let search_in_other_index: Option<(usize, u64)> = self.messages.iter().enumerate().find_map(|(p, i)| {
                if let Some(s_item) = i {
                    if s_item.item_ver.eq(&version) {
                        return Some((p, version));
                    }
                }
                None
            });
            if let Some(s_item) = search_in_other_index {
                error!(
                    "Version {version} was found at another index {} while it was expected at index {:?} for suffix head {}. This could lead to ambigious behaviour of suffix.",
                    s_item.0,
                    self.index_from_head(version),
                    self.get_meta().head
                )
            }
            // info!("All some items on suffix.... {:?}", self.retrieve_all_some_vec_items());
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

    fn update_metrics(&self) {
        if let Some(m) = self.g_suffix_length.as_ref() {
            m.record(self.suffix_length() as u64, &[])
        }
        if let Some(m) = self.g_suffix_head.as_ref() {
            m.record(self.meta.head, &[])
        }
    }
}

impl<T> SuffixTrait<T> for Suffix<T>
where
    T: Sized + Clone + std::fmt::Debug,
{
    fn get(&self, version: u64) -> SuffixResult<Option<SuffixItem<T>>> {
        let index = self.index_from_head(version).ok_or(SuffixError::VersionToIndexConversionError(version))?;
        let suffix_item = self.messages.get(index).and_then(|x| x.as_ref()).cloned();

        if suffix_item.is_none() {
            debug!("Could not find version {version} at suffix index {index}");
        }

        Ok(suffix_item)
    }

    fn get_mut(&mut self, version: u64) -> Option<&mut SuffixItem<T>> {
        let index = self.index_from_head(version)?;
        self.messages.get_mut(index)?.as_mut()
    }

    fn get_meta(&self) -> &SuffixMeta {
        &self.meta
    }

    fn insert(&mut self, version: u64, message: T) -> SuffixResult<()> {
        // Version 0 is not a valid version in suffix and therefore will be ignored.
        if version == 0 {
            info!("Version {version} will be ignored.");
            self.update_metrics();
            return Ok(());
        }

        // The very first item inserted on the suffix will automatically be made head of the suffix.
        if self.meta.head == 0 {
            self.update_head(version);
            info!("Suffix head set to {version}");
            self.messages.push_back(Some(SuffixItem {
                item: message,
                item_ver: version,
                decision_ver: None,
                is_decided: false,
            }));
            debug!("Inserted version {version} as the first item to suffix");
            self.update_metrics();
            return Ok(());
        }

        // If the version to insert is less than the head of suffix, it is safe to discard.
        if self.meta.head.gt(&version) {
            info!(
                "Skipped inserting into suffix as message version received ({}) is below the suffix head version ({})",
                self.meta.head, version
            );
            self.update_metrics();
            return Ok(());
        }

        let last_inserted_version = self.meta.last_insert_vers;
        // If the version to insert is less than the last_inserted_version (tail of suffix), we skip it.
        // Version numbers should be moving forward. So there are only two possible reasons for this to happen:
        // - The message with the version is send again.
        // - Upstream has passed an incorrect version.
        // In either case it is safe to discard this message.
        if last_inserted_version > 0 && version.le(&last_inserted_version) {
            info!("Skipped inserting into suffix as message version received ({version}) is below the suffix tail version ({last_inserted_version}).");
            self.update_metrics();
            return Ok(());
        }

        // Good to insert.
        self.reserve_space_if_required(version)?;
        let index = self.index_from_head(version).ok_or(SuffixError::ItemNotFound(version, None))?;
        let last_item_index = self.messages.len() - 1;

        // empty_slots is used to fill empty indices in the suffix with None between the current tail and the new version being inserted
        let empty_slots = if index > last_item_index { (index - last_item_index) as i32 } else { 0 };
        if empty_slots > 0 {
            let suffix_length_before_empty_slots = self.messages.len();
            for _ in 0..empty_slots {
                self.messages.push_back(None);
            }
            let suffix_length_after_empty_slots = self.messages.len();
            self.messages[index] = Some(SuffixItem {
                item: message,
                item_ver: version,
                decision_ver: None,
                is_decided: false,
            });

            self.meta.last_insert_vers = version;
            debug!(
                "Inserted version {version} at calculated index on suffix = {index} | actual index on suffix = {} | before insert suffix length = {suffix_length_before_empty_slots} | after insert suffix length = {suffix_length_after_empty_slots} | empty_slots added = {}",
                self.messages.len() - 1,
                empty_slots - 1,
            );
        } else {
            warn!("Failed to insert version {version} at index {index}");
        }

        self.update_metrics();
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
        let suffix_before = format!("head={}, length={}", self.meta.head, self.suffix_length());

        let drain_started_at = Instant::now();
        let (drained_entries, new_head) = match self.messages.range(index..).skip(1).find(|m| m.is_some()) {
            Some(Some(next_item)) => {
                let next_version = next_item.item_ver;
                let next_index = self.index_from_head(next_version).unwrap();
                let drained: Vec<Option<SuffixItem<T>>> = self.messages.drain(..next_index).collect();

                (drained, next_version)
            }
            _ => {
                let drained = self.messages.drain(..).collect();
                self.update_head(0);
                (drained, 0)
            }
        };

        let drain_end_micros = drain_started_at.elapsed().as_micros();
        let updated_started_at = Instant::now();
        self.update_prune_index(None);
        self.update_head(new_head);
        let updated_end_micros = updated_started_at.elapsed().as_micros();
        info!(
            "Suffix pruned (till index {}). Old value: {}, New value: head={}, length={}. Drained items={}. Duration(micros): drain={}, update={}",
            index,
            suffix_before,
            self.meta.head,
            self.suffix_length(),
            drained_entries.len(),
            drain_end_micros,
            updated_end_micros
        );

        self.update_metrics();
        Ok(drained_entries)
    }

    fn prune_till_version(&mut self, version: u64) -> SuffixResult<Vec<Option<SuffixItem<T>>>> {
        let suffix_before = format!("head={}, length={}", self.meta.head, self.suffix_length());
        let mut prune_result: SuffixResult<Vec<Option<SuffixItem<T>>>> = Ok(vec![]);

        if let Some(index) = self.index_from_head(version) {
            debug!("Index send for pruning is {index} for version={version}");
            let prune_index = self.find_prune_till_index(index);
            prune_result = self.prune_till_index(prune_index);
            debug!("Items on suffix after pruning = {:#?}", self.retrieve_all_some_vec_items().len());
            debug!("Suffix after - prune head = {} | length = {}", self.meta.head, self.suffix_length());
            let pruned_count = if let Ok(prune_items) = prune_result.as_ref() { prune_items.len() } else { 0 };

            info!(
                "Suffix pruned (till version {}). Old value: {}, New value: head={}, length={}. Pruned items={}",
                version,
                suffix_before,
                self.meta.head,
                self.suffix_length(),
                pruned_count,
            );
        } else {
            let search_in_other_index: Option<(usize, u64)> = self.messages.iter().enumerate().find_map(|(p, i)| {
                if let Some(s_item) = i {
                    if s_item.item_ver.eq(&version) {
                        return Some((p, version));
                    }
                }
                None
            });

            if let Some(s_item) = search_in_other_index {
                error!(
                    "Unable to prune (till version {}) as index not found by 'search from head' method. This tem is at index: {}. Current suffix: {}. This could lead to ambigious behaviour of suffix.",
                    version, s_item.0, suffix_before,
                );
            } else {
                warn!(
                    "Unable to prune (till version {}) as index not found. Current suffix: {}",
                    version, suffix_before
                );
            }
        }

        self.update_metrics();
        prune_result
    }

    fn remove(&mut self, version: u64) -> SuffixResult<()> {
        let index = self.index_from_head(version).ok_or(SuffixError::ItemNotFound(version, None))?;

        self.messages[index] = None;

        // if we are removing the pruneable version, set prune_vers to None.
        if self.meta.prune_index == Some(index) {
            self.update_prune_index(None);
        }

        self.update_metrics();
        Ok(())
    }
}
