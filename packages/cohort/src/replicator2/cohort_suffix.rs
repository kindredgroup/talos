use std::collections::VecDeque;

use talos_certifier::model::Decision;
use talos_suffix::{
    core::{SuffixConfig, SuffixMeta},
    errors::SuffixError,
};

use super::model::ReplicatorCandidate2;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SuffixItem {
    /// Data received from Kafka Consumer
    pub item: ReplicatorCandidate2,
    pub item_ver: u64,
    pub decision_ver: Option<u64>,
    /// Flag to denote if this item has been decided (is_decided is set to true irrespective of the outcome).
    pub is_decided: bool,
    pub is_in_flight: bool,
    pub is_installed: bool,
    pub decided_at: Option<i128>,
}

pub struct CohortSuffix {
    pub meta: SuffixMeta,
    pub messages: VecDeque<Option<SuffixItem>>,
}

impl CohortSuffix {
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
    pub fn with_config(config: SuffixConfig) -> CohortSuffix {
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

        CohortSuffix { meta, messages }
    }

    pub fn print(&self) {
        log::warn!("<<<<<<.....");
        log::warn!("{:?}", self.meta);
        for (i, d) in self.messages.iter().enumerate() {
            if let Some(entry) = d {
                if !entry.is_installed {
                    log::warn!(
                        "{}: {}, d={}, fl={}, ins={}, {:?}",
                        i,
                        entry.item_ver,
                        entry.is_decided,
                        entry.is_in_flight,
                        entry.is_installed,
                        entry.item.decision
                    )
                }
            } else {
                //log::warn!("{}: {:?}", i, d);
            }
        }
        log::warn!(".....>>>>>>");
    }

    fn get(&mut self, version: u64) -> Result<Option<SuffixItem>, SuffixError> {
        let index = self.index_from_head(version).ok_or(SuffixError::VersionToIndexConversionError(version))?;
        let suffix_item = self.messages.get(index).and_then(|x| x.as_ref()).cloned();

        Ok(suffix_item)
    }

    fn update_head(&mut self, version: u64) {
        self.meta.head = version;
    }

    /// Reserve space when the version we are inserting is
    /// outside the upper bounds of suffix.
    ///
    /// Reserves space and defaults them with None.
    fn reserve_space_if_required(&mut self, version: u64) -> Result<(), SuffixError> {
        let ver_diff: usize = (version - self.meta.head) as usize + 1;

        if ver_diff > self.messages.len() {
            // let resize_len = if ver_diff < self.meta.min_size { self.meta.min_size + 1 } else { ver_diff };
            self.messages.reserve(ver_diff + 1);
        }

        Ok(())
    }

    fn index_from_head(&self, version: u64) -> Option<usize> {
        let head = self.meta.head;
        if version < head {
            None
        } else {
            Some((version - head) as usize)
        }
    }

    pub fn insert(&mut self, version: u64, data: ReplicatorCandidate2) -> Result<(), SuffixError> {
        // he very first item inserted on the suffix will automatically be made head of the suffix.
        if self.meta.head == 0 {
            self.update_head(version);
        }

        if self.meta.head <= version {
            self.reserve_space_if_required(version)?;
            let index = self.index_from_head(version).ok_or(SuffixError::ItemNotFound(version, None))?;

            if index > 0 {
                let last_item_index = self.index_from_head(self.meta.last_insert_vers).unwrap_or(0);
                for _ in (last_item_index + 1)..index {
                    self.messages.push_back(None);
                }
            }

            self.messages.push_back(Some(SuffixItem {
                item: data,
                item_ver: version,
                decision_ver: None,
                is_decided: false,
                is_in_flight: false,
                is_installed: false,
                decided_at: None,
            }));

            self.meta.last_insert_vers = version;
        }

        Ok(())
    }

    pub fn update_decision(&mut self, version: u64, decision_ver: u64, decided_at: i128) -> Result<(), SuffixError> {
        // When Certifier is catching up with messages ignore the messages which are prior to the head
        if version < self.meta.head {
            log::info!("Returned due to version < self.meta.head for version={version} and decision version={decision_ver}");
            return Ok(());
        }

        let Some(sfx_item) = self.get(version)? else {
            log::info!("Returned due item not found in suffix for version={version} with index={:?}  and decision version={decision_ver}", self.index_from_head(version));
                return Ok(());
            };

        let new_sfx_item = SuffixItem {
            decision_ver: Some(decision_ver),
            is_decided: true,
            decided_at: Some(decided_at),
            ..sfx_item
        };

        let index = self
            .index_from_head(version)
            .ok_or(SuffixError::IndexCalculationError(self.meta.head, version))?;

        log::debug!("Updating version={version} with index={index:?} and decision version={decision_ver}");
        self.messages[index] = Some(new_sfx_item);
        Ok(())
    }

    pub fn set_decision_outcome(&mut self, version: u64, decision_outcome: Decision) {
        if version >= self.meta.head {
            let index = self.index_from_head(version).unwrap();
            if let Some(Some(item_to_update)) = self.messages.get_mut(index) {
                item_to_update.item.decision = Some(decision_outcome);
            } else {
                log::warn!(
                    "Unable to update decision as message with version={version} not found. len: {}, index: {}, head: {}",
                    self.messages.len(),
                    index,
                    self.meta.head
                );
            }
        }
    }

    pub fn set_safepoint(&mut self, version: u64, safepoint: Option<u64>) {
        if version >= self.meta.head {
            let index = self.index_from_head(version).unwrap();
            if let Some(Some(item_to_update)) = self.messages.get_mut(index) {
                item_to_update.item.safepoint = safepoint;
            } else {
                log::warn!("Unable to update safepoint as message with version={version} not found");
            }
        }
    }

    pub fn set_item_installed(&mut self, version: u64) {
        if version >= self.meta.head {
            let index = self.index_from_head(version).unwrap();
            if let Some(Some(item_to_update)) = self.messages.get_mut(index) {
                item_to_update.is_installed = true;
                item_to_update.is_in_flight = false;
            } else {
                log::warn!("Unable to update is_installed flag as message with version={version} not found");
            }
        }
    }

    pub fn set_item_in_flight(&mut self, version: u64) {
        if version >= self.meta.head {
            let index = self.index_from_head(version).unwrap();
            if let Some(Some(item_to_update)) = self.messages.get_mut(index) {
                item_to_update.is_in_flight = true;
            } else {
                log::warn!("Unable to update is_in_flight flag as message with version={version} not found");
            }
        }
    }

    pub fn find_new_decided(&mut self, last_in_flight: Option<u64>, skip: bool) -> Option<SuffixItem> {
        let skip_count = if skip {
            if let Some(last_in_flight) = last_in_flight {
                match self.index_from_head(last_in_flight) {
                    None => 0,
                    Some(index) => index + 1,
                }
            } else {
                0
            }
        } else {
            0
        };

        self.messages
            .iter()
            .skip(skip_count)
            .flatten()
            .take_while(|i| i.is_decided)
            .filter(|i| !i.is_installed && !i.is_in_flight)
            .take(1)
            .last()
            .cloned()
    }

    pub fn find_commit_offset(&mut self, last_in_flight: Option<u64>) -> Option<u64> {
        let skip_count = if let Some(last_in_flight) = last_in_flight {
            match self.index_from_head(last_in_flight) {
                None => 0,
                Some(index) => index + 1,
            }
        } else {
            0
        };

        self.messages
            .iter()
            .skip(skip_count)
            .flatten()
            .take_while(|i| i.is_decided && i.is_installed)
            .take(1)
            .map(|i| i.item_ver)
            .last()
    }

    pub fn update_prune_index(&mut self, version: u64) {
        if self.are_prior_items_decided(version) {
            let index = self.index_from_head(version).unwrap();
            self.meta.prune_index = Some(index);
        }
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
        self.messages.range(range).flatten().all(|k| k.is_decided)
    }

    pub fn get_suffix_meta(&self) -> &SuffixMeta {
        &self.meta
    }

    pub fn prune_till_version(&mut self, version: u64) -> Result<Vec<Option<SuffixItem>>, SuffixError> {
        log::debug!("Suffix before prune.... {}", self.suffix_length());
        if let Some(index) = self.index_from_head(version) {
            log::debug!("Index send for pruning is {index} for version={version}");
            let prune_index = self.find_prune_till_index(index);
            let prune_result = self.prune_till_index(prune_index);
            log::debug!("Suffix items pruned.... {prune_result:?}");
            log::debug!("Suffix after prune.... {}", self.suffix_length());
            // log::debug!("Items on suffix after pruning = {:#?}", self.retrieve_all_some_vec_items());
            return prune_result;
        } else {
            log::warn!("Unable to prune as index not found for version {version}.")
        }
        Ok(vec![])
    }

    fn suffix_length(&self) -> usize {
        self.messages.len()
    }

    fn find_prune_till_index(&mut self, prune_till_index: usize) -> usize {
        let prune_till_index = self
            .messages
            .range(..prune_till_index + 1)
            .enumerate()
            .rev()
            .find_map(|(i, x)| x.is_some().then_some(i))
            .unwrap();

        prune_till_index
    }

    /// Prune the suffix
    ///
    /// Looks at the meta to find the prune ready version.
    ///
    /// Prune is allowed when
    ///     1. The meta has a valid prune version.
    ///     2. And there is atleast one suffix item remaining, which can be the new head.
    ///        This enables to move the head to the appropiate location.
    fn prune_till_index(&mut self, index: usize) -> Result<Vec<Option<SuffixItem>>, SuffixError> {
        log::debug!("Suffix message length BEFORE pruning={} and head={}!!!", self.messages.len(), self.meta.head);

        let drained_entries = self.messages.drain(..index).collect();
        self.meta.prune_index = None;
        if let Some(Some(s_item)) = self.messages.iter().find(|m| m.is_some()) {
            self.update_head(s_item.item_ver);
        } else {
            self.update_head(0)
        }
        Ok(drained_entries)
    }

    fn retrieve_all_some_vec_items(&self) -> Vec<(usize, u64, Option<u64>)> {
        self.messages
            .iter()
            .enumerate()
            .filter_map(|(i, x)| x.is_some().then(|| (i, x.as_ref().unwrap().item_ver, x.as_ref().unwrap().decision_ver)))
            .collect()
    }
}
