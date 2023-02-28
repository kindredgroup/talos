use talos_suffix::SuffixItem;

use crate::{
    certifier::{
        certification::{Certifier, CertifyOutcome, Discord, Outcome},
        utils::convert_vec_to_hashmap,
        CertifierCandidate,
    },
    model::CandidateReadWriteSet,
};

use super::utils::generate_certifier_sets_from_suffix;

#[test]
fn test_certify_rule_1() {
    let certify_tx = CertifierCandidate {
        vers: 0,
        snapshot: 0,
        readvers: vec![],
        readset: vec![],
        writeset: vec![],
    };

    let mut certifier = Certifier::new();
    let test_writes = convert_vec_to_hashmap(vec![("g:1", 0), ("g:2", 0)]);
    Certifier::update_set(&mut certifier.writes, test_writes);

    // test the writes are added to the certifier.writes
    assert_eq!(certifier.writes.len(), 2);

    // test certifier made outcome based on rule 1
    assert!(certifier.reads.is_empty());
    assert_eq!(certifier.certify(0, &certify_tx), CertifyOutcome::Commited { discord: Discord::Assertive });
}

#[test]
fn test_certify_rule_2() {
    let mut certifier = Certifier::new();

    let test_reads = convert_vec_to_hashmap(vec![("g:1", 0), ("g:2", 0), ("g:3", 2), ("l:2", 4)]);
    let test_writes = convert_vec_to_hashmap(vec![("g:1", 0), ("g:2", 0)]);

    Certifier::update_set(&mut certifier.reads, test_reads);
    Certifier::update_set(&mut certifier.writes, test_writes);

    let certify_tx_b = CertifierCandidate {
        vers: 9,
        snapshot: 0,
        readvers: vec![2, 3],
        readset: vec!["g1".to_owned()],
        writeset: vec!["l1".to_owned()],
    };

    assert_eq!(
        certifier.certify(4, &certify_tx_b),
        CertifyOutcome::Aborted {
            version: None,
            discord: Discord::Permissive
        }
    );
}

#[test]
fn test_certify_rule_3() {
    let mut certifier = Certifier::new();
    let test_reads = convert_vec_to_hashmap(vec![("g:1", 0), ("g:2", 0), ("g:3", 2), ("l:2", 4)]);

    let test_writes = convert_vec_to_hashmap(vec![
        ("g:1", 0),
        ("g:2", 6), // anit-dependecy here
    ]);
    Certifier::update_set(&mut certifier.reads, test_reads);
    Certifier::update_set(&mut certifier.writes, test_writes);

    let certify_tx_b = CertifierCandidate {
        vers: 9,
        snapshot: 5,
        readvers: vec![2, 3],
        readset: vec!["g1".to_owned(), "g:2".to_owned()],
        writeset: vec!["l1".to_owned()],
    };

    assert_eq!(
        certifier.certify(4, &certify_tx_b),
        CertifyOutcome::Aborted {
            version: Some(6),
            discord: Discord::Assertive
        }
    );
}

#[test]
fn test_certify_rule_4() {
    let mut certifier = Certifier::new();

    let test_reads = convert_vec_to_hashmap(vec![("g:1", 0), ("g:2", 0), ("g:3", 2), ("l:2", 4)]);
    let test_writes = convert_vec_to_hashmap(vec![
        ("g:1", 0),
        ("g:2", 6), // anit-dependecy here
    ]);
    Certifier::update_set(&mut certifier.reads, test_reads);
    Certifier::update_set(&mut certifier.writes, test_writes);

    let certify_tx_b = CertifierCandidate {
        vers: 9,
        snapshot: 6,
        readvers: vec![2, 3],
        readset: vec!["g1".to_owned(), "g:2".to_owned()],
        writeset: vec!["l1".to_owned()],
    };

    assert_eq!(certifier.certify(4, &certify_tx_b), CertifyOutcome::Commited { discord: Discord::Permissive });
}

#[test]
fn test_certify_all_4_rules() {
    let mut certifier = Certifier::new();
    let test_reads = convert_vec_to_hashmap(vec![("g:1", 0), ("g:2", 0), ("g:3", 2), ("l:2", 4)]);
    let test_writes = convert_vec_to_hashmap(vec![("g:1", 0), ("g:2", 3)]);

    Certifier::update_set(&mut certifier.reads, test_reads);
    Certifier::update_set(&mut certifier.writes, test_writes);

    //test rule 1
    let certify_tx = CertifierCandidate {
        vers: 8,
        snapshot: 6,
        readvers: vec![2, 3],
        readset: vec![],
        writeset: vec!["l1".to_owned()],
    };

    assert_eq!(certifier.certify(4, &certify_tx), CertifyOutcome::Commited { discord: Discord::Assertive });

    Certifier::update_set(&mut certifier.reads, certify_tx.convert_readset_to_collection());
    Certifier::update_set(&mut certifier.writes, certify_tx.convert_writeset_to_collection());

    //test rule 2
    let certify_tx = CertifierCandidate {
        vers: 10,
        snapshot: 2,
        readvers: vec![2, 3],
        readset: vec!["k2".to_owned(), "k4".to_owned()],
        writeset: vec!["l2".to_owned()],
    };

    assert_eq!(
        certifier.certify(4, &certify_tx),
        CertifyOutcome::Aborted {
            version: None,
            discord: Discord::Permissive
        }
    );

    Certifier::update_set(&mut certifier.reads, certify_tx.convert_readset_to_collection());
    Certifier::update_set(&mut certifier.writes, certify_tx.convert_writeset_to_collection());

    //test rule 4
    let certify_tx = CertifierCandidate {
        vers: 15,
        snapshot: 9,
        readvers: vec![2, 3],
        readset: vec!["k9".to_owned(), "k22".to_owned()],
        writeset: vec!["l7".to_owned(), "l1".to_owned()],
    };

    assert_eq!(certifier.certify(4, &certify_tx), CertifyOutcome::Commited { discord: Discord::Permissive });

    Certifier::update_set(&mut certifier.reads, certify_tx.convert_readset_to_collection());
    Certifier::update_set(&mut certifier.writes, certify_tx.convert_writeset_to_collection());

    //test rule 3 - anti-dependency
    let certify_tx = CertifierCandidate {
        vers: 20,
        snapshot: 9,
        readvers: vec![2, 3, 7],
        readset: vec!["k9".to_owned(), "k22".to_owned(), "l2".to_owned()],
        writeset: vec!["l7".to_owned()],
    };

    assert_eq!(
        certifier.certify(8, &certify_tx),
        CertifyOutcome::Aborted {
            version: Some(10),
            discord: Discord::Assertive
        }
    );
}

#[test]
fn calculate_safe_point_rule_1() {
    let certify_tx = CertifierCandidate {
        vers: 25,
        snapshot: 18,
        readvers: vec![],
        readset: vec![],
        writeset: vec![],
    };

    let mut certifier = Certifier::new();

    let test_writes = convert_vec_to_hashmap(vec![("g:1", 12), ("g:2", 18)]);
    let test_reads = convert_vec_to_hashmap(vec![("k:1", 13), ("k:2", 22)]);

    Certifier::update_set(&mut certifier.reads, test_reads);
    Certifier::update_set(&mut certifier.writes, test_writes);

    // test this is rule 1
    assert_eq!(certifier.certify(0, &certify_tx), CertifyOutcome::Commited { discord: Discord::Assertive });

    // testing safepoints
    assert_eq!(certifier.calculate_safe_point(0, &certify_tx), 24);
    assert_eq!(certifier.calculate_safe_point(12, &certify_tx), 11);
}

#[test]
fn calculate_safe_point_rule_4() {
    let mut certifier = Certifier::new();

    let test_writes = convert_vec_to_hashmap(vec![("g1", 12), ("g2", 18)]);
    let test_reads = convert_vec_to_hashmap(vec![("k1", 13), ("k2", 28)]);

    Certifier::update_set(&mut certifier.reads, test_reads);
    Certifier::update_set(&mut certifier.writes, test_writes);

    let certify_tx = CertifierCandidate {
        vers: 30,
        snapshot: 18,
        readvers: vec![19, 12],
        readset: vec!["k2".to_owned(), "g6".to_owned()],
        writeset: vec!["k3".to_owned()],
    };
    // test this is rule 1
    assert_eq!(certifier.certify(12, &certify_tx), CertifyOutcome::Commited { discord: Discord::Permissive });

    // testing safepoints
    assert_eq!(certifier.calculate_safe_point(20, &certify_tx), 28);
    assert_eq!(certifier.calculate_safe_point(12, &certify_tx), 28);

    Certifier::update_set(&mut certifier.reads, certify_tx.convert_readset_to_collection());
    Certifier::update_set(&mut certifier.writes, certify_tx.convert_writeset_to_collection());

    let certify_tx = CertifierCandidate {
        vers: 33,
        snapshot: 29,
        readvers: vec![19, 12],
        readset: vec!["k2".to_owned(), "g6".to_owned()],
        writeset: vec!["k3".to_owned()],
    };

    // test this is rule 1
    assert_eq!(certifier.certify(12, &certify_tx), CertifyOutcome::Commited { discord: Discord::Permissive });
}
#[test]
fn test_update_set() {
    let mut certifier = Certifier::new();

    let certifier_writes = convert_vec_to_hashmap(vec![("g1", 12), ("g2", 18)]);
    let certifier_reads = convert_vec_to_hashmap(vec![("k1", 13), ("k2", 28)]);

    Certifier::update_set(&mut certifier.reads, certifier_reads);
    Certifier::update_set(&mut certifier.writes, certifier_writes);

    assert_eq!(certifier.reads.get(&"k1".to_owned()).unwrap(), &13);
    assert_eq!(certifier.writes.get(&"g1".to_owned()).unwrap(), &12);
}
#[test]
fn test_prune_set() {
    let mut certifier = Certifier::new();

    let certifier_writes = convert_vec_to_hashmap(vec![("g1", 12), ("g2", 18), ("l1", 22), ("l2", 28), ("m1", 30), ("m2", 31)]);

    Certifier::update_set(&mut certifier.writes, certifier_writes);

    // test all keys in prune hashmap is present in certifier write hashmap
    let remove_writes = convert_vec_to_hashmap(vec![("g1", 12), ("m2", 31)]);
    Certifier::prune_set(&mut certifier.writes, &remove_writes);
    assert_eq!(certifier.writes.len(), 4);

    // test not all keys in prune hashmap is present in certifier write hashmap
    let remove_writes = convert_vec_to_hashmap(vec![("g2", 12), ("mk", 31)]);
    Certifier::prune_set(&mut certifier.writes, &remove_writes);
    assert_eq!(certifier.writes.len(), 3);
}

#[test]
fn test_certifying_txn() {
    let mut certifier = Certifier::new();
    let test_reads = convert_vec_to_hashmap(vec![
        ("a1", 5),
        ("a2", 6),
        ("a3", 10),
        ("b2", 2),
        ("c1", 22),
        ("c2", 12),
        ("c3", 13),
        ("d2", 23),
        ("e1", 9),
        ("e2", 10),
        ("e3", 12),
        ("l2", 14),
        ("g1", 30),
        ("g2", 33),
        ("g3", 23),
        ("l2", 43),
    ]);
    let test_writes = convert_vec_to_hashmap(vec![
        ("k1", 9),
        ("k2", 16),
        ("k3", 10),
        ("l2", 21),
        ("r1", 20),
        ("l1", 32),
        ("e43", 19),
        ("lf2", 14),
        ("gd1", 37),
        ("gds2", 43),
        ("g23", 44),
        ("lsd2", 49),
    ]);
    Certifier::update_set(&mut certifier.reads, test_reads);
    Certifier::update_set(&mut certifier.writes, test_writes);

    let certify_tx = CertifierCandidate {
        vers: 55,
        snapshot: 20,
        readvers: vec![10, 29],
        readset: vec!["r1".to_owned(), "e43".to_owned()],
        writeset: vec!["l1".to_owned()],
    };
    let outcome = certifier.certify_transaction(18, certify_tx);

    assert_eq!(
        outcome,
        Outcome::Commited {
            safepoint: 32,
            discord: Discord::Permissive
        }
    )
}

struct MockCandidateReadWriteSetItem {
    readset: Vec<String>,
    writeset: Vec<String>,
}

impl CandidateReadWriteSet for MockCandidateReadWriteSetItem {
    fn get_readset(&self) -> &Vec<String> {
        &self.readset
    }

    fn get_writeset(&self) -> &Vec<String> {
        &self.writeset
    }
}

fn generate_basic_mock_set_item(prefix: &str, vers: u64, suffix: &str) -> String {
    format!("{prefix}:{vers}:{suffix}")
}

fn generate_data_for_util_test(vers: u64, readset: Option<Vec<String>>, writeset: Option<Vec<String>>) -> SuffixItem<MockCandidateReadWriteSetItem> {
    SuffixItem {
        item: MockCandidateReadWriteSetItem {
            readset: readset.unwrap_or_else(|| vec![generate_basic_mock_set_item("rs", vers, "1")]),
            writeset: writeset.unwrap_or_else(|| vec![generate_basic_mock_set_item("ws", vers, "1")]),
        },
        item_ver: vers,
        decision_ver: None,
        is_decided: false,
    }
}

#[test]
fn test_util_generate_certifier_sets_from_suffix_success_with_no_duplicates() {
    let test_data = (0..10_u64)
        .map(|v| generate_data_for_util_test(v, None, None))
        .collect::<Vec<SuffixItem<MockCandidateReadWriteSetItem>>>();
    let (readset, writeset) = generate_certifier_sets_from_suffix(test_data.iter());

    assert_eq!(readset.len(), 10);
    assert_eq!(writeset.len(), 10);

    assert_eq!(*readset.get(&"rs:9:1".to_string()).unwrap(), 9);
    assert_eq!(*writeset.get(&"ws:7:1".to_string()).unwrap(), 7);
}
#[test]
fn test_util_generate_certifier_sets_from_suffix_success_with_duplicates() {
    let test_data = (0..10_u64)
        .map(|v| {
            if v % 2 == 0 {
                return generate_data_for_util_test(v, None, None);
            }
            // read and write sets get duplicated to v-1 version.
            generate_data_for_util_test(
                v,
                Some(vec![generate_basic_mock_set_item("rs", v - 1, "1")]),
                Some(vec![generate_basic_mock_set_item("ws", v - 1, "1")]),
            )
        })
        .collect::<Vec<SuffixItem<MockCandidateReadWriteSetItem>>>();
    let (readset, writeset) = generate_certifier_sets_from_suffix(test_data.iter());

    assert_eq!(readset.len(), 5);
    assert_eq!(writeset.len(), 5);

    assert!(readset.get(&"rs:9:1".to_string()).is_none()); // when v % 2 = 0, we store on key v-1
    assert_eq!(*readset.get(&"rs:8:1".to_string()).unwrap(), 9);

    assert_eq!(*writeset.get(&"ws:6:1".to_string()).unwrap(), 7);
}
#[test]
fn test_util_generate_certifier_sets_from_empty_suffix_read_write_sets() {
    let test_data = (0..10_u64)
        .map(|v| generate_data_for_util_test(v, Some(vec![]), Some(vec![])))
        .collect::<Vec<SuffixItem<MockCandidateReadWriteSetItem>>>();
    let (readset, writeset) = generate_certifier_sets_from_suffix(test_data.iter());

    assert_eq!(readset.len(), 0);
    assert_eq!(writeset.len(), 0);
}
