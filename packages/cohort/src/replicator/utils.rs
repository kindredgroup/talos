use talos_suffix::SuffixItem;

use super::core::{ReplicatorSuffixItemTrait, StatemapItem};

pub fn get_filtered_batch<'a, T: ReplicatorSuffixItemTrait + 'a>(messages: impl Iterator<Item = &'a SuffixItem<T>>) -> impl Iterator<Item = &'a SuffixItem<T>> {
    messages
        .into_iter()
        .filter(|&m| m.item.get_safepoint().is_some()) // select only the messages that have safepoint i.e committed messages
        .filter(|&m| m.item.get_statemap().is_some()) // select only the messages that have statemap.
}

pub fn get_statemap_from_suffix_items<'a, T: ReplicatorSuffixItemTrait + 'a>(messages: impl Iterator<Item = &'a SuffixItem<T>>) -> Vec<StatemapItem> {
    messages.into_iter().fold(vec![], |mut acc, m| {
        m.item.get_statemap().as_ref().unwrap().iter().for_each(|sm| {
            let key = sm.keys().next().unwrap().to_string();
            let payload = sm.get(&key).unwrap().clone();
            acc.push(StatemapItem {
                action: key,
                payload,
                version: m.item_ver,
            })
        });
        acc
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use talos_certifier::model::CandidateDecisionOutcome;
    use talos_suffix::SuffixItem;

    use crate::replicator::core::ReplicatorSuffixItemTrait;

    use super::get_filtered_batch;

    #[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
    struct TestCandidate {
        #[serde(skip_deserializing)]
        pub safepoint: Option<u64>,

        #[serde(skip_deserializing)]
        pub decision_outcome: Option<CandidateDecisionOutcome>,

        #[serde(skip_serializing_if = "Option::is_none")]
        pub statemap: Option<Vec<HashMap<String, Value>>>,
    }

    impl ReplicatorSuffixItemTrait for TestCandidate {
        fn get_safepoint(&self) -> &Option<u64> {
            &self.safepoint
        }

        fn get_statemap(&self) -> &Option<Vec<HashMap<String, Value>>> {
            &self.statemap
        }

        fn set_safepoint(&mut self, safepoint: Option<u64>) {
            self.safepoint = safepoint
        }

        fn set_decision_outcome(&mut self, decision_outcome: Option<talos_certifier::model::CandidateDecisionOutcome>) {
            self.decision_outcome = decision_outcome
        }
    }

    #[test]
    fn test_get_filtered_batch_all_pass() {
        let value = serde_json::from_str(
            r#"{
             "s": {}
            }"#,
        )
        .unwrap();

        let mut statemap_item = HashMap::new();
        statemap_item.insert("k".to_owned(), value);

        let suffix_item = SuffixItem {
            item: TestCandidate {
                safepoint: Some(1),
                decision_outcome: None,
                statemap: Some(vec![statemap_item]),
            },
            item_ver: 2,
            decision_ver: None,
            is_decided: false,
        };

        let result = get_filtered_batch(vec![&suffix_item].into_iter());

        assert_eq!(result.count(), 1);
    }
}
