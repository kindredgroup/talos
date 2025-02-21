use std::collections::{HashMap, VecDeque};

use log::info;
use serde_json::Value;
use talos_suffix::{core::SuffixMeta, Suffix, SuffixTrait};

use crate::{
    core::CandidateDecisionOutcome,
    suffix::{ReplicatorSuffixItemTrait, ReplicatorSuffixTrait},
};

#[derive(Debug, Default, PartialEq, Clone)]
struct TestReplicatorSuffixItem {
    safepoint: Option<u64>,
    decision: Option<CandidateDecisionOutcome>,
    statemap: Option<Vec<std::collections::HashMap<String, serde_json::Value>>>,
    is_installed: bool,
}

impl ReplicatorSuffixItemTrait for TestReplicatorSuffixItem {
    fn get_safepoint(&self) -> &Option<u64> {
        &self.safepoint
    }

    fn get_statemap(&self) -> &Option<Vec<std::collections::HashMap<String, serde_json::Value>>> {
        &self.statemap
    }

    fn set_safepoint(&mut self, safepoint: Option<u64>) {
        self.safepoint = safepoint
    }

    fn set_decision_outcome(&mut self, decision_outcome: Option<CandidateDecisionOutcome>) {
        self.decision = decision_outcome
    }
    fn set_suffix_item_installed(&mut self) {
        self.is_installed = true
    }

    fn is_installed(&self) -> bool {
        self.is_installed
    }
}

#[test]
fn test_replicator_suffix_item() {
    let mut suffix_item = TestReplicatorSuffixItem::default();

    // test - safepoint
    assert!(suffix_item.get_safepoint().is_none());
    suffix_item.set_safepoint(Some(120));
    assert_eq!(suffix_item.get_safepoint(), &Some(120));

    // test - statemap
    assert!(suffix_item.get_statemap().is_none());

    let mut statemap_item = HashMap::new();
    statemap_item.insert("k".to_owned(), Value::Bool(true));
    suffix_item.statemap = Some(vec![statemap_item]);

    assert!(suffix_item.get_statemap().is_some());

    // test - decision_outcome
    assert!(suffix_item.decision.is_none());
    suffix_item.set_decision_outcome(Some(CandidateDecisionOutcome::Committed));
}

#[test]
fn test_replicator_suffix() {
    let suffix_messages = VecDeque::new();

    let mut suffix: Suffix<TestReplicatorSuffixItem> = Suffix {
        meta: SuffixMeta::default(),
        messages: suffix_messages,
    };

    assert_eq!(suffix.messages.len(), 0);
    suffix.insert(3, TestReplicatorSuffixItem::default()).unwrap();
    assert_eq!(suffix.messages.len(), 1);

    suffix.insert(5, TestReplicatorSuffixItem::default()).unwrap();
    suffix.insert(8, TestReplicatorSuffixItem::default()).unwrap();

    // Message batch is empty as the decision is not added.
    assert!(suffix.get_message_batch_from_version(5, Some(5)).is_empty());

    // Nothing happens for version 50 updates as the item doesn't exist.
    suffix.set_safepoint(50, Some(2));
    suffix.set_decision_outcome(50, Some(CandidateDecisionOutcome::Committed));

    //add safepoint and decision for version 3
    suffix.set_safepoint(3, Some(2));
    suffix.set_decision_outcome(3, Some(CandidateDecisionOutcome::Committed));
    suffix.update_decision(3, 10).unwrap();

    let item_at_version3 = suffix.get(3).unwrap().unwrap();
    assert_eq!(item_at_version3.item.safepoint.unwrap(), 2);
    assert_eq!(item_at_version3.item.decision.unwrap(), CandidateDecisionOutcome::Committed);
    assert!(!item_at_version3.item.is_installed);
    // Message batch will be one as only version 3's decision is recorded..
    assert_eq!(suffix.get_message_batch_from_version(0, None).len(), 1);

    suffix.update_decision(4, 12).unwrap();
    // Message batch will still be 1 as there was no version 1 inserted.
    // So the decision will be discarded
    assert_eq!(suffix.get_message_batch_from_version(0, Some(4)).len(), 1);

    suffix.update_decision(5, 19).unwrap();
    // Message batch will be 2 as safepoint is not set a decision is made, therefore version 3 and 4 are picked.
    // version 3 is considered as commited as the safepoint and decision_outcome is set.
    // version 4 is considered as aborted at this point as safepoint is not set.
    assert_eq!(suffix.get_message_batch_from_version(0, Some(10)).len(), 2);
    assert_eq!(suffix.get_message_batch_from_version(3, Some(10)).len(), 1);

    //add safepoint and decision for version 8
    suffix.update_decision(8, 19).unwrap();
    suffix.set_safepoint(8, Some(2));
    suffix.set_decision_outcome(8, Some(CandidateDecisionOutcome::Committed));
    // Message batch will be 3, as version 3,5, and 8 are not installed.
    assert_eq!(suffix.get_message_batch_from_version(0, Some(10)).len(), 3);

    //add safepoint and decision for version 5
    suffix.set_safepoint(5, Some(2));
    suffix.set_decision_outcome(5, Some(CandidateDecisionOutcome::Committed));
    // Message batch will be 3  as version 3, 4 and 5 has Some safepoint value
    assert_eq!(suffix.get_message_batch_from_version(0, Some(10)).len(), 3);
}

#[test]
fn test_replicator_suffix_installed() {
    let suffix_messages = VecDeque::new();

    let mut suffix: Suffix<TestReplicatorSuffixItem> = Suffix {
        meta: SuffixMeta::default(),
        messages: suffix_messages,
    };

    assert_eq!(suffix.messages.len(), 0);
    suffix.insert(3, TestReplicatorSuffixItem::default()).unwrap();
    suffix.insert(6, TestReplicatorSuffixItem::default()).unwrap();
    suffix.insert(9, TestReplicatorSuffixItem::default()).unwrap();

    // update decision for version 3
    suffix.update_suffix_item_decision(3, 19).unwrap();
    suffix.set_safepoint(3, Some(2));
    suffix.set_decision_outcome(3, Some(CandidateDecisionOutcome::Committed));

    // Batch returns one item as only version 3 is decided, others haven't got the decisions yet.
    assert_eq!(suffix.get_message_batch_from_version(0, Some(1)).len(), 1);
    suffix.set_item_installed(3);
    // Batch returns 0 items as version 3 is already installed, others haven't got the decisions yet.
    assert!(suffix.get_message_batch_from_version(0, Some(1)).is_empty());

    let suffix_item_3 = suffix.get(3).unwrap().unwrap();
    // confirm version 3 is marked as installed.
    assert!(suffix_item_3.item.is_installed());

    // update decision for version 9
    suffix.update_suffix_item_decision(9, 23).unwrap();
    suffix.set_safepoint(9, Some(2));
    suffix.set_decision_outcome(9, Some(CandidateDecisionOutcome::Committed));
    // Batch returns 0, because there is a version in between which is not decided.
    assert!(suffix.get_message_batch_from_version(0, Some(1)).is_empty());

    // update decision for version 6
    suffix.update_suffix_item_decision(6, 23).unwrap();
    suffix.set_safepoint(6, None);
    suffix.set_decision_outcome(6, Some(CandidateDecisionOutcome::Aborted));
    // Batch returns 2 items (version 6 &  9).
    let batch = suffix.get_message_batch_from_version(0, None);
    assert_eq!(batch.len(), 2);

    // Confirm the batch returned the correct item.
    assert_eq!(batch.first().unwrap().item_ver, 6);

    // Mark version 9 as installed.
    suffix.set_item_installed(9);
    // Although version 9 is installed, version 6 is not, therefore it is picked up here.
    assert_eq!(suffix.get_message_batch_from_version(3, Some(1)).len(), 1);

    assert_eq!(suffix.get_suffix_meta().head, 3);
}

// Prior to Feb 2025, the commit offset was derived using suffix.get_last_installed.
// But this function is expensive as the suffix grows.
// Therefore we just use the `prune_index` as we know till that point everything is decided and installed.
#[test]
fn test_get_offset_to_commit_from_replicator_suffix() {
    let suffix_messages = VecDeque::new();

    let mut suffix: Box<dyn ReplicatorSuffixTrait<TestReplicatorSuffixItem>> = Box::new(Suffix {
        meta: SuffixMeta::default(),
        messages: suffix_messages,
    });

    // Insert 200 items into suffix
    for vers in 0..=200 {
        suffix.insert(vers, TestReplicatorSuffixItem::default()).unwrap();
    }
    assert_eq!(suffix.get_meta().head, 1);
    //  Mark few of them as decided
    // Till version 79 is decided
    for vers in 0..80 {
        suffix.update_suffix_item_decision(vers, vers + 9_999).unwrap();
        suffix.set_safepoint(vers, Some(vers));
        suffix.set_decision_outcome(vers, Some(CandidateDecisionOutcome::Committed));
    }

    // Till version 199 is decided
    for vers in 90..=200 {
        suffix.update_suffix_item_decision(vers, vers + 9_999).unwrap();
        suffix.set_safepoint(vers, Some(vers));
        suffix.set_decision_outcome(vers, Some(CandidateDecisionOutcome::Committed));
    }

    // 80 - 89 versions are not decided, but all other items till version 200 are decided
    suffix.update_prune_index(200);

    // prune_index is None as items are not installed
    assert_eq!(suffix.get_meta().prune_index, None);

    if let Some(prune_index) = suffix.get_meta().prune_index {
        if let Some(item) = suffix.get_by_index(prune_index) {
            let version = item.item_ver;
            suffix.prune_till_version(version).unwrap();
            assert_eq!(suffix.get_meta().head, version);
        }
    }
    // head hasn't moved as no prune yet.
    // last_installed version is None as no items are installed.
    assert_eq!(suffix.get_last_installed(Some(30)), None);

    // Versions 0 to 30 are installed.
    for vers in 0..=30 {
        suffix.set_item_installed(vers);
    }

    suffix.update_prune_index(30);

    // prune_index is Some(30) as only till version 30 is installed
    assert_eq!(suffix.get_meta().prune_index, Some(29));
    if let Some(prune_index) = suffix.get_meta().prune_index {
        if let Some(item) = suffix.get_by_index(prune_index) {
            let version = item.item_ver;
            info!("Version {version} got by get_by_index for index {prune_index}");
            suffix.prune_till_version(version).unwrap();
            assert_eq!(suffix.get_meta().head, version);
            assert_eq!(suffix.get_meta().head, 30);
            assert_eq!(suffix.get_meta().prune_index, None);
        }
    }

    // Versions between 80 - 90 are decided
    for vers in 80..90 {
        suffix.update_suffix_item_decision(vers, vers + 9_999).unwrap();
        suffix.set_safepoint(vers, Some(vers));
        suffix.set_decision_outcome(vers, Some(CandidateDecisionOutcome::Committed));
    }

    // Installed version 31 to 47.
    for vers in 31..=47 {
        suffix.set_item_installed(vers);
    }

    suffix.update_prune_index(200);

    // Couldn't set prune index as version 200 is not installed.
    assert_eq!(suffix.get_meta().prune_index, None);

    suffix.update_prune_index(47);
    // Since till version 47 is installed, and head is at 30, we know prune_index will be at 17.
    assert_eq!(suffix.get_meta().prune_index, Some(17));
    if let Some(prune_index) = suffix.get_meta().prune_index {
        if let Some(item) = suffix.get_by_index(prune_index) {
            let version = item.item_ver;
            suffix.prune_till_version(version).unwrap();
            assert_eq!(suffix.get_meta().head, version);
            assert_eq!(suffix.get_meta().head, 47);
            assert_eq!(suffix.get_meta().prune_index, None);
        }
    }

    // Install versions 50 to 200
    for vers in 50..=200 {
        suffix.set_item_installed(vers);
    }

    suffix.update_prune_index(200);
    // Couldn't set prune index as version 200 as version 48 and 49 are not installed.
    assert_eq!(suffix.get_meta().prune_index, None);

    suffix.set_item_installed(48);
    suffix.set_item_installed(49);
    suffix.update_prune_index(200);

    assert_eq!(suffix.get_meta().head, 47);
    // pruned index = index of version 200 from head. 200 - 47 = 153
    assert_eq!(suffix.get_meta().prune_index, Some(153));

    // Prune the suffix till prune_index.
    if let Some(prune_index) = suffix.get_meta().prune_index {
        if let Some(item) = suffix.get_by_index(prune_index) {
            let version = item.item_ver;
            suffix.prune_till_version(version).unwrap();
            assert_eq!(suffix.get_meta().head, version);
        }
    }
    assert_eq!(suffix.get_meta().head, 200);
    assert_eq!(suffix.get_meta().prune_index, None);
}
