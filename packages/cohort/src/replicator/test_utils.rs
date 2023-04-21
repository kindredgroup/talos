use rand::{seq::SliceRandom, thread_rng};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use talos_certifier::model::CandidateDecisionOutcome;
use talos_suffix::SuffixItem;

use super::core::ReplicatorSuffixItemTrait;

fn generate_bank_transfer_statemap_value() -> Value {
    let accounts_vec = (0..10).collect::<Vec<u32>>();
    let accounts_slice = accounts_vec.as_slice();

    let amounts_slice: &[u32] = &[100, 120, 200, 300, 450];

    let mut rng = thread_rng();
    let first_account_suffix = accounts_slice.choose(&mut rng).unwrap();

    let second_account_suffix = loop {
        let random_suffix = accounts_slice.choose(&mut rng).unwrap();
        if random_suffix != first_account_suffix {
            break random_suffix;
        }
    };

    let amount = amounts_slice.choose(&mut rng).unwrap();

    json!(format!(
        r#"
                {{
                    from: account-{},
                    to: account-{},
                    amount: {}
                }}
            "#,
        first_account_suffix, second_account_suffix, amount
    ))
}

pub(crate) fn generate_test_statemap<F>(action: &str, value_generator_fn: F) -> HashMap<String, Value>
where
    F: Fn() -> Value,
{
    let mut statemap_item = HashMap::new();
    statemap_item.insert(action.to_owned(), value_generator_fn());
    statemap_item
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub(crate) struct BankStatemapTestCandidate {
    pub safepoint: Option<u64>,

    pub decision_outcome: Option<CandidateDecisionOutcome>,

    pub statemap: Option<Vec<HashMap<String, Value>>>,
}

impl Default for BankStatemapTestCandidate {
    fn default() -> Self {
        Self {
            safepoint: Default::default(),
            decision_outcome: Default::default(),
            statemap: Some(vec![generate_test_statemap("transfer", generate_bank_transfer_statemap_value)]),
        }
    }
}

impl BankStatemapTestCandidate {
    pub(crate) fn set_statemap(mut self, statemap: Option<Vec<HashMap<String, Value>>>) -> Self {
        self.statemap = statemap;
        self
    }

    pub(crate) fn set_safepoint(mut self, safepoint: Option<u64>) -> Self {
        ReplicatorSuffixItemTrait::set_safepoint(&mut self, safepoint);
        self
    }

    pub(crate) fn generate_bank_transfers_statemap(mut self, count: u32) -> Self {
        // self.statemap = Some(vec![]);

        let statemap = (0..count).map(|_| generate_test_statemap("transfer", generate_bank_transfer_statemap_value));

        self.statemap = Some(statemap.collect());
        self
    }
}

impl ReplicatorSuffixItemTrait for BankStatemapTestCandidate {
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

pub(crate) fn build_test_suffix_item<T: ReplicatorSuffixItemTrait>(version: u64, decision_ver: Option<u64>, item: T) -> SuffixItem<T> {
    SuffixItem {
        item,
        item_ver: version,
        decision_ver,
        is_decided: decision_ver.is_some(),
    }
}
