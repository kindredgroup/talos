use std::collections::VecDeque;

use log::{debug, info, warn};
use talos_suffix::{
    core::{SuffixConfig, SuffixItemType, SuffixMeta, SuffixResult},
    errors::SuffixError,
    get_nonempty_suffix_items, SuffixItem, SuffixTrait,
};

use crate::replicator::core::{CandidateMessage, DecisionOutcome};

pub trait SuffixDecisionTrait<T> {
    fn set_decision(&mut self, version: u64, decision_outcome: Option<DecisionOutcome>);
    fn set_safepoint(&mut self, version: u64, safepoint: Option<u64>);
    fn get_message_batch(&self) -> Option<Vec<&SuffixItem<T>>>;
}

#[derive(Debug)]
pub struct ReplicatorSuffix<T> {
    pub meta: SuffixMeta,
    pub messages: VecDeque<Option<SuffixItem<T>>>,
}

impl<T> ReplicatorSuffix<T>
where
    T: Sized + Clone + std::fmt::Debug,
{
    pub fn with_config(config: SuffixConfig) -> ReplicatorSuffix<T> {
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

        ReplicatorSuffix { meta, messages }
    }

    fn update_head(&mut self, version: u64) {
        self.meta.head = version;
    }

    fn index_from_head(&self, version: u64) -> Option<usize> {
        let head = self.meta.head;
        if version < head {
            None
        } else {
            Some((version - head) as usize)
        }
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

impl SuffixDecisionTrait<CandidateMessage> for ReplicatorSuffix<CandidateMessage> {
    fn set_decision(&mut self, version: u64, decision_outcome: Option<DecisionOutcome>) {
        if version >= self.meta.head {
            let index = self.index_from_head(version).unwrap();
            if let Some(item_to_update) = self.messages.get_mut(index).unwrap() {
                item_to_update.item.decision_outcome = decision_outcome;
            } else {
                warn!("Unable to update decision as message with version={version} not found");
            }
        }
    }

    fn set_safepoint(&mut self, version: u64, safepoint: Option<u64>) {
        if version >= self.meta.head {
            let index = self.index_from_head(version).unwrap();
            if let Some(item_to_update) = self.messages.get_mut(index).unwrap() {
                item_to_update.item.safepoint = safepoint;
            } else {
                warn!("Unable to update safepoint as message with version={version} not found");
            }
        }
    }

    fn get_message_batch(&self) -> Option<Vec<&SuffixItem<CandidateMessage>>> {
        if let Some(prune_index) = self.meta.prune_index {
            let batch_messages = self.messages.range(..prune_index).flatten().collect::<Vec<&SuffixItem<CandidateMessage>>>();
            return Some(batch_messages);
        }
        None
    }
}

impl<T> SuffixTrait<T> for ReplicatorSuffix<T>
where
    T: Sized + Clone + std::fmt::Debug,
{
    fn get(&mut self, version: u64) -> SuffixResult<Option<talos_suffix::SuffixItem<T>>> {
        let index = self.index_from_head(version).ok_or(SuffixError::VersionToIndexConversionError(version))?;
        let suffix_item = self.messages.get(index).and_then(|x| x.as_ref()).cloned();

        Ok(suffix_item)
    }

    fn insert(&mut self, version: u64, message: SuffixItemType<T>) -> SuffixResult<()> {
        // skip zero version
        if version > 0 {
            if self.meta.head == 0 {
                self.update_head(version);
            }
            if self.meta.head.le(&version) {
                // self.reserve_space_if_required(version)?;
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
        } else {
            warn!("Zero message will not be inserted");
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
            let index = self.index_from_head(version).unwrap();
            self.update_prune_index(Some(index));
        }

        Ok(())
    }

    fn prune_till_index(&mut self, _index: usize) -> SuffixResult<Vec<Option<SuffixItem<T>>>> {
        todo!()
    }

    fn remove(&mut self, _version: u64) -> SuffixResult<()> {
        todo!()
    }
}
