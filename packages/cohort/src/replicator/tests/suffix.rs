use std::collections::{HashMap, VecDeque};

use serde_json::Value;
use talos_suffix::{core::SuffixMeta, Suffix, SuffixTrait};

use crate::replicator::suffix::{ReplicatorSuffixItemTrait, ReplicatorSuffixTrait};

#[derive(Debug, Default, PartialEq, Clone)]
struct TestReplicatorSuffixItem {
    safepoint: Option<u64>,
    decision: Option<talos_certifier::model::CandidateDecisionOutcome>,
    statemap: Option<Vec<std::collections::HashMap<String, serde_json::Value>>>,
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

    fn set_decision_outcome(&mut self, decision_outcome: Option<talos_certifier::model::CandidateDecisionOutcome>) {
        self.decision = decision_outcome
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
    suffix_item.set_decision_outcome(Some(talos_certifier::model::CandidateDecisionOutcome::Committed));
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
    assert!(suffix.get_message_batch().is_none());

    // Nothing happens for version 50 updates as the item doesn't exist.
    suffix.set_safepoint(50, Some(2));
    suffix.set_decision_outcome(50, Some(talos_certifier::model::CandidateDecisionOutcome::Committed));

    //add safepoint and decision for version 3
    suffix.set_safepoint(3, Some(2));
    suffix.set_decision_outcome(3, Some(talos_certifier::model::CandidateDecisionOutcome::Committed));

    let item_at_version3 = suffix.get(3).unwrap().unwrap();
    assert_eq!(item_at_version3.item.safepoint.unwrap(), 2);
    assert_eq!(
        item_at_version3.item.decision.unwrap(),
        talos_certifier::model::CandidateDecisionOutcome::Committed
    );
    suffix.update_decision(3, 10).unwrap();
    // Message batch will be one as only version 3's decision is recorded..
    assert_eq!(suffix.get_message_batch().unwrap().len(), 1);

    suffix.update_decision(4, 12).unwrap();
    // Message batch will still be 1 as there was no version 1 inserted.
    // So the decision will be discarded
    assert_eq!(suffix.get_message_batch().unwrap().len(), 1);

    suffix.update_decision(5, 19).unwrap();
    // Message batch will be 2
    assert_eq!(suffix.get_message_batch().unwrap().len(), 2);
}
