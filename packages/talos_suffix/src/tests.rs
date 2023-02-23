// --- tests
#[cfg(test)]
mod suffix_tests {

    use std::ops::Range;

    use crate::{
        core::{SuffixConfig, SuffixTrait},
        Suffix,
    };

    #[derive(Debug, Clone)]
    #[allow(dead_code)]
    struct MockSuffixItemMessage {
        xid: String,
        agent: String,
        cohort: String,
        vers: u64,
    }

    fn create_mock_candidate_message(vers: u64) -> MockSuffixItemMessage {
        let xid = format!("xid-{}", vers);
        let agent = format!("agent-{}", vers);
        let cohort = format!("cohort-{}", vers);
        MockSuffixItemMessage { xid, agent, cohort, vers }
    }

    fn filtered_versions_vec(r: Range<u64>, ignore_vers: Option<Vec<u64>>) -> Vec<u64> {
        let full_vec = r.collect::<Vec<u64>>();

        let Some(ignore_vec) = ignore_vers else {
            return  full_vec;
        };

        let vec_remaining = full_vec.iter().filter_map(|&v| (!ignore_vec.contains(&v)).then_some(v));

        vec_remaining.collect::<Vec<u64>>()
    }

    #[test]
    fn test_with_config_with_all_config_values() {
        let sfx: Suffix<MockSuffixItemMessage> = Suffix::with_config(SuffixConfig {
            capacity: 20,
            prune_start_threshold: Some(15),
            min_size_after_prune: Some(10),
        });

        assert_eq!(sfx.meta.head, 0);
        assert_eq!(sfx.meta.prune_start_threshold, Some(15));
        assert_eq!(sfx.meta.prune_index, None);
        assert_eq!(sfx.meta.min_size_after_prune, Some(10));
        assert!(sfx.messages.capacity() > 20);
        assert_eq!(sfx.messages.len(), 0);
    }

    #[test]
    #[should_panic(expected = "The config min_size_after=Some(10) is greater than prune_start_threshold=None")]
    fn test_with_config_when_prune_start_threshold_is_none_but_min_size_is_some() {
        let _sfx: Suffix<MockSuffixItemMessage> = Suffix::with_config(SuffixConfig {
            capacity: 20,
            min_size_after_prune: Some(10),
            ..Default::default()
        });
    }

    #[test]
    fn test_with_config_when_min_size_is_none_and_prune_start_threshold_is_some() {
        let sfx: Suffix<MockSuffixItemMessage> = Suffix::with_config(SuffixConfig {
            capacity: 20,
            prune_start_threshold: Some(10),
            ..Default::default()
        });

        assert_eq!(sfx.meta.prune_start_threshold, Some(10));
        assert_eq!(sfx.meta.prune_index, None);
        assert_eq!(sfx.meta.min_size_after_prune, None);
    }

    #[test]
    #[should_panic(expected = "The config min_size_after=Some(10) is greater than prune_start_threshold=Some(5)")]
    fn test_with_config_when_prune_start_threshold_is_less_than_min_size() {
        let _sfx: Suffix<MockSuffixItemMessage> = Suffix::with_config(SuffixConfig {
            capacity: 20,
            min_size_after_prune: Some(10),
            prune_start_threshold: Some(5),
        });
    }

    // fn test_is_valid_prune_version_index()
    // fn test_is_valid_prune_version_index_false()

    // fn test_reserve_space_if_required()

    // fn test_are_prior_items_decided_false()
    // fn test_are_prior_items_decided_true_prune_index...index()
    // fn test_are_prior_items_decided_true_0...index()

    #[test]
    fn test_is_ready_for_prune_returns_false_when_prune_start_threshold_is_not_set() {
        let mut sfx: Suffix<MockSuffixItemMessage> = Suffix::with_config(SuffixConfig {
            capacity: 5,
            ..Default::default()
        });

        // insert suffix items
        for vers in 0..=80 {
            sfx.insert(vers, create_mock_candidate_message(vers)).unwrap();
        }

        // update suffix items
        for decision_ver in 81..=121 {
            sfx.update_decision(decision_ver - 81, decision_ver).unwrap();
        }

        // because `prune_start_threshold` is `None` prune doesn't happen.
        assert_eq!(sfx.messages.len(), 81);

        assert_eq!(sfx.meta.prune_index, Some(39));
        assert!(!sfx.is_ready_for_prune());
    }

    #[test]
    fn test_is_ready_for_returns_false_when_suffix_len_less_than_prune_threshold() {
        let mut sfx: Suffix<MockSuffixItemMessage> = Suffix::with_config(SuffixConfig {
            capacity: 30,
            prune_start_threshold: Some(28),
            min_size_after_prune: Some(10),
        });

        // insert suffix items
        for vers in 0..=20 {
            sfx.insert(vers, create_mock_candidate_message(vers)).unwrap();
        }

        // update suffix items
        for decision_ver in 21..=35 {
            sfx.update_decision(decision_ver - 21, decision_ver).unwrap();
        }

        // because `prune_start_threshold` is `None` prune doesn't happen.
        assert_eq!(sfx.messages.len(), 21);

        assert_eq!(sfx.meta.prune_index, Some(13));
        assert!(!sfx.is_ready_for_prune());
    }

    #[test]
    fn test_is_ready_for_prune_returns_false_when_prune_index_is_none() {
        let mut sfx: Suffix<MockSuffixItemMessage> = Suffix::with_config(SuffixConfig {
            capacity: 30,
            prune_start_threshold: Some(28),
            min_size_after_prune: Some(10),
        });

        // insert suffix items
        for vers in 0..=20 {
            sfx.insert(vers, create_mock_candidate_message(vers)).unwrap();
        }

        // because `prune_start_threshold` is `None` prune doesn't happen.
        assert_eq!(sfx.messages.len(), 21);

        assert_eq!(sfx.meta.prune_index, None);
        assert!(!sfx.is_ready_for_prune());
    }

    #[test]
    fn test_is_ready_for_prune_when_min_size_is_none() {
        let mut sfx: Suffix<MockSuffixItemMessage> = Suffix::with_config(SuffixConfig {
            capacity: 30,
            prune_start_threshold: Some(20),
            ..Default::default()
        });

        // insert suffix items
        for vers in 0..30 {
            sfx.insert(vers, create_mock_candidate_message(vers)).unwrap();
        }

        // update suffix items
        for decision_ver in 30..=55 {
            sfx.update_decision(decision_ver - 30, decision_ver).unwrap();
        }

        // because `prune_start_threshold` is `None` prune doesn't happen.
        assert_eq!(sfx.messages.len(), 30);

        assert_eq!(sfx.meta.prune_index, Some(24));
        assert!(sfx.is_ready_for_prune());
    }

    #[test]
    fn test_is_ready_for_prune_true_with_prune_index_updated_to_nearest() {
        let mut sfx: Suffix<MockSuffixItemMessage> = Suffix::with_config(SuffixConfig {
            capacity: 30,
            prune_start_threshold: Some(20),
            ..Default::default()
        });

        // insert suffix items
        let ignore_vec: Vec<u64> = vec![18, 22, 26, 27, 29];

        let filtered_vec = filtered_versions_vec(0..31, Some(ignore_vec));

        filtered_vec.iter().for_each(|&vers| {
            sfx.insert(vers, create_mock_candidate_message(vers)).unwrap();
        });

        assert_eq!(sfx.messages.len(), 31);

        filtered_vec.iter().for_each(|&vers| {
            sfx.update_decision(vers, vers + 30).unwrap();
        });

        assert_eq!(sfx.meta.prune_index, Some(16)); // because version 18, index 17 is not decided.
        assert!(sfx.is_ready_for_prune()); // because message.len > prune_start_threshold, we are ready for prune till the prune_index
    }

    // test the following while pruning
    // - returned items in suffix len and version
    // - remaining items in suffix len and version
    // - updated head value
    // - updated prune version number
    // fn test_prune

    #[test]
    fn get_index_from_head() {
        let mut sfx = Suffix::<MockSuffixItemMessage>::with_config(SuffixConfig {
            capacity: 20,
            ..Default::default()
        });

        assert_eq!(sfx.index_from_head(10).unwrap(), 10);

        sfx.meta.head = 11;
        assert_eq!(sfx.index_from_head(11).unwrap(), 0);
        assert_eq!(sfx.index_from_head(13).unwrap(), 2);
    }
    #[test]
    fn insert_version_within_dq_len() {
        let mut sfx = Suffix::with_config(SuffixConfig {
            capacity: 1000,
            ..Default::default()
        });
        assert_eq!(sfx.messages.len(), 0);
        // Initial insert.
        sfx.insert(33, create_mock_candidate_message(33)).unwrap();

        let sfxi = sfx.get(33).unwrap().unwrap();

        assert_eq!(sfxi.item.xid, create_mock_candidate_message(33).xid);
        assert_eq!(sfx.messages.len(), 1);

        // Another insert.
        sfx.insert(38, create_mock_candidate_message(38)).unwrap();
        assert_eq!(sfx.messages.len(), 6);
    }

    #[test]
    fn check_version_safe_to_prune_when_all_prior_is_empty() {
        let ver: u64 = 20;
        let mut sfx = Suffix::with_config(SuffixConfig {
            capacity: 20,
            ..Default::default()
        });
        sfx.insert(ver, create_mock_candidate_message(ver)).unwrap();

        assert!(sfx.are_prior_items_decided(ver));
    }
    #[test]
    fn check_version_safe_to_prune_when_prior_undecided() {
        let ver: u64 = 20;
        let mut sfx = Suffix::with_config(SuffixConfig {
            capacity: 20,
            ..Default::default()
        });
        sfx.insert(10, create_mock_candidate_message(10)).unwrap();
        sfx.insert(ver, create_mock_candidate_message(ver)).unwrap();

        assert!(!sfx.are_prior_items_decided(ver));
    }
    #[test]
    fn check_version_safe_to_prune_when_prior_decided() {
        let mut sfx = Suffix::with_config(SuffixConfig {
            capacity: 20,
            ..Default::default()
        });
        let ver_10: u64 = 10;
        sfx.insert(ver_10, create_mock_candidate_message(ver_10)).unwrap();
        let result = sfx.update_decision_suffix_item(ver_10, 23);

        assert!(result.is_ok());

        let ver_18: u64 = 18;
        sfx.insert(ver_18, create_mock_candidate_message(ver_18)).unwrap();
        sfx.insert(20, create_mock_candidate_message(20)).unwrap();

        assert!(sfx.are_prior_items_decided(ver_18));
    }

    #[test]
    fn update_decision_within_bounds_and_item_exists() {
        env_logger::init();

        let mut sfx = Suffix::with_config(SuffixConfig {
            capacity: 20,
            ..Default::default()
        });
        let ver_10: u64 = 10;
        let decision_ver: u64 = 12;
        sfx.insert(ver_10, create_mock_candidate_message(ver_10)).unwrap();

        let _ = sfx.update_decision(ver_10, decision_ver);

        let ver_10_message = sfx.get(ver_10).unwrap().unwrap();

        assert_eq!(ver_10_message.item_ver, ver_10);
        assert_eq!(ver_10_message.decision_ver.unwrap(), decision_ver);

        let ver_18: u64 = 18;
        sfx.insert(ver_18, create_mock_candidate_message(ver_18)).unwrap();

        let _ = sfx.update_decision(ver_18, 20);

        assert_eq!(sfx.meta.prune_index.unwrap(), 8); //9th item, i.e index = 8
    }

    #[test]
    fn update_decision_when_item_not_in_suffix() {
        let mut sfx = Suffix::<MockSuffixItemMessage>::with_config(SuffixConfig {
            capacity: 20,
            ..Default::default()
        });
        let ver_10: u64 = 10;
        let decision_ver: u64 = 23;

        let result = sfx.update_decision_suffix_item(ver_10, decision_ver);

        assert!(result.is_ok());

        let ver_25: u64 = 25;
        let decision_ver: u64 = 29;

        let result = sfx.update_decision_suffix_item(ver_25, decision_ver);

        assert!(result.is_ok());
    }

    // #[test]
    // fn prune_items() {
    //     let mut sfx = Suffix::new(10, 20);
    //     let ver_10: u64 = 10;
    //     sfx.insert(ver_10, create_mock_candidate_message(ver_10)).unwrap();

    //     let ver_25: u64 = 25;
    //     sfx.insert(ver_25, create_mock_candidate_message(ver_25)).unwrap();

    //     let decision_ver: u64 = 23;
    //     let _ = sfx.update_decision(ver_10, decision_ver);

    //     let decision_ver: u64 = 29;
    //     let _ = sfx.update_decision(ver_25, decision_ver);

    //     let ver_33: u64 = 33;
    //     sfx.insert(ver_33, create_mock_candidate_message(ver_33)).unwrap();

    //     assert_eq!(sfx.messages.len(), 34);
    //     assert_eq!(sfx.meta.prune_index.unwrap(), 25);

    //     sfx.prune().unwrap();

    //     assert!(sfx.meta.prune_index.is_none());
    //     assert_eq!(sfx.meta.head, 33);
    //     assert_eq!(sfx.messages.len(), 11);

    //     let sfx_v33 = sfx.get(33).unwrap().unwrap();
    //     assert_eq!(sfx_v33.item_ver, 33);

    //     let decision_ver: u64 = 39;
    //     let result = sfx.update_decision(ver_33, decision_ver);

    //     assert!(result.is_ok());

    //     assert!(sfx.meta.prune_index.is_some());

    //     sfx.prune().unwrap();

    //     assert_eq!(sfx.messages.len(), 11);
    // }
}
