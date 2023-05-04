use std::collections::{HashMap, VecDeque};

use serde_json::Value;
use talos_suffix::{core::SuffixMeta, Suffix, SuffixTrait};

use crate::replicator::{
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
    assert_eq!(suffix.get_message_batch(Some(5)), None);

    // Nothing happens for version 50 updates as the item doesn't exist.
    suffix.set_safepoint(50, Some(2));
    suffix.set_decision_outcome(50, Some(CandidateDecisionOutcome::Committed));

    //add safepoint and decision for version 3
    suffix.set_safepoint(3, Some(2));
    suffix.set_decision_outcome(3, Some(CandidateDecisionOutcome::Committed));

    let item_at_version3 = suffix.get(3).unwrap().unwrap();
    assert_eq!(item_at_version3.item.safepoint.unwrap(), 2);
    assert_eq!(item_at_version3.item.decision.unwrap(), CandidateDecisionOutcome::Committed);
    suffix.update_decision(3, 10).unwrap();
    // Message batch will be one as only version 3's decision is recorded..
    assert_eq!(suffix.get_message_batch(Some(5)).unwrap().len(), 1);

    suffix.update_decision(4, 12).unwrap();
    // Message batch will still be 1 as there was no version 1 inserted.
    // So the decision will be discarded
    assert_eq!(suffix.get_message_batch(Some(4)).unwrap().len(), 1);

    suffix.update_decision(5, 19).unwrap();
    // Message batch will be 1 as safepoint is not set for version 4 and version 5
    assert_eq!(suffix.get_message_batch(Some(10)).unwrap().len(), 1);

    //add safepoint and decision for version 8
    suffix.update_decision(8, 19).unwrap();
    suffix.set_safepoint(8, Some(2));
    suffix.set_decision_outcome(8, Some(CandidateDecisionOutcome::Committed));
    // Message batch will be 1  as version 4 doesn't have a safepoint yet.
    assert_eq!(suffix.get_message_batch(Some(10)).unwrap().len(), 2);

    //add safepoint and decision for version 5
    suffix.set_safepoint(5, Some(2));
    suffix.set_decision_outcome(5, Some(CandidateDecisionOutcome::Committed));
    // Message batch will be 3  as version 3, 4 and 5 has Some safepoint value
    assert_eq!(suffix.get_message_batch(Some(10)).unwrap().len(), 3);
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
    assert_eq!(suffix.get_message_batch(Some(1)).unwrap().len(), 1);
    suffix.set_item_installed(3);
    // Batch returns 0 items as version 3 is already installed, others haven't got the decisions yet.
    assert!(suffix.get_message_batch(Some(1)).is_none());

    let suffix_item_3 = suffix.get(3).unwrap().unwrap();
    // confirm version 3 is marked as installed.
    assert!(suffix_item_3.item.is_installed());

    // update decision for version 9
    suffix.update_suffix_item_decision(9, 23).unwrap();
    suffix.set_safepoint(9, Some(2));
    suffix.set_decision_outcome(9, Some(CandidateDecisionOutcome::Committed));
    // Batch returns 0, because there is a version in between which is not decided.
    assert!(suffix.get_message_batch(Some(1)).is_none());

    // update decision for version 6
    suffix.update_suffix_item_decision(6, 23).unwrap();
    suffix.set_safepoint(6, None);
    suffix.set_decision_outcome(6, Some(CandidateDecisionOutcome::Aborted));
    // Batch returns 1 item (version 9), because version 6 decision is aborted.
    let batch = suffix.get_message_batch(None).unwrap();
    assert_eq!(batch.len(), 1);

    // Confirm the batch returned the correct item.
    assert_eq!(batch.first().unwrap().item_ver, 9);

    // Mark version 9 as installed.
    suffix.set_item_installed(9);
    assert!(suffix.get_message_batch(Some(1)).is_none());
}
