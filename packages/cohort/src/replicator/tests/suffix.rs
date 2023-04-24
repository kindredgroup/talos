use std::collections::HashMap;

use serde_json::Value;

use crate::replicator::suffix::ReplicatorSuffixItemTrait;

#[derive(Debug, Default, PartialEq)]
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
fn test_replicator_suffix() {
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
