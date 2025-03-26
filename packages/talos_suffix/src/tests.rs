// --- tests
#[cfg(test)]
mod suffix_tests {

    use std::ops::Range;

    use crate::{
        core::{SuffixConfig, SuffixTrait},
        utils::get_nonempty_suffix_items,
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
            return full_vec;
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

    #[test]
    fn test_insert_duplicate_versions() {
        let mut sfx: Suffix<MockSuffixItemMessage> = Suffix::with_config(SuffixConfig {
            capacity: 5,
            ..Default::default()
        });

        // insert suffix items
        for vers in 0..=20 {
            sfx.insert(vers, create_mock_candidate_message(vers)).unwrap();
        }

        assert_eq!(sfx.messages.len(), 20);

        // insert duplicated suffix items
        for vers in 10..15 {
            sfx.insert(vers, create_mock_candidate_message(vers)).unwrap();
        }
        assert_eq!(sfx.messages.len(), 20);
    }
    #[test]
    fn test_insert_versions_with_empty_indices() {
        let mut sfx: Suffix<MockSuffixItemMessage> = Suffix::with_config(SuffixConfig {
            capacity: 5,
            ..Default::default()
        });

        // insert suffix items
        for vers in 0..20 {
            sfx.insert(vers, create_mock_candidate_message(vers)).unwrap();
        }

        assert_eq!(sfx.messages.len(), 19);

        // insert duplicated suffix items
        for vers in 30..35 {
            sfx.insert(vers, create_mock_candidate_message(vers)).unwrap();
        }
        assert_eq!(sfx.messages.len(), 34);
    }
    #[test]
    fn test_insert_versions_with_empty_indices_duplicates() {
        let mut sfx: Suffix<MockSuffixItemMessage> = Suffix::with_config(SuffixConfig {
            capacity: 5,
            ..Default::default()
        });

        // insert suffix items
        for vers in 0..20 {
            sfx.insert(vers, create_mock_candidate_message(vers)).unwrap();
        }

        assert_eq!(sfx.messages.len(), 19);

        // insert duplicated suffix items
        for vers in 10..15 {
            sfx.insert(vers, create_mock_candidate_message(vers)).unwrap();
        }

        // insert duplicated suffix items
        for vers in 30..35 {
            sfx.insert(vers, create_mock_candidate_message(vers)).unwrap();
        }
        assert_eq!(sfx.messages.len(), 34);
    }
    #[test]
    fn test_insert_versions_with_empty_indices_duplicates_and_prune() {
        let mut sfx: Suffix<MockSuffixItemMessage> = Suffix::with_config(SuffixConfig {
            capacity: 5,
            prune_start_threshold: Some(10),
            ..Default::default()
        });

        // insert suffix items
        for vers in 1..=20 {
            sfx.insert(vers, create_mock_candidate_message(vers)).unwrap();
        }

        assert_eq!(sfx.messages.len(), 20);
        assert_eq!(sfx.meta.head, 1);

        let suffix_len = sfx.messages.len() as u64;
        // update decisions
        for vers in 21..=35 {
            sfx.update_decision(vers - suffix_len, vers).unwrap();
        }

        assert_eq!(sfx.meta.prune_index, Some(14)); // version 15 at index 14

        let _ = sfx.prune_till_index(sfx.get_safe_prune_index().unwrap());
        // As prune_start_threshold is after 10 items in suffix, and safe to prune upto index is 14, i.e version 15. Head will move from 1 -> 15.
        assert_eq!(sfx.meta.head, 15);
        assert_eq!(sfx.messages.len(), 6);

        // insert suffix items after prune head is at 15, so from 15 to 35 i.e inserting 21 items into suffix. Of which 15 - 20 are duplicates
        // Therefore they should be ignored and only the rest should be inserting, and the total length should become 21.
        for vers in 15..=35 {
            sfx.insert(vers, create_mock_candidate_message(vers)).unwrap();
        }

        assert_eq!(sfx.messages.len(), 21);
    }

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
        assert_eq!(sfx.messages.len(), 80);

        assert_eq!(sfx.meta.prune_index, Some(39));
        assert!(sfx.get_safe_prune_index().is_none());
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
        assert_eq!(sfx.messages.len(), 20);

        assert_eq!(sfx.meta.prune_index, Some(13));
        assert!(sfx.get_safe_prune_index().is_none());
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
        assert_eq!(sfx.messages.len(), 20);

        assert_eq!(sfx.meta.prune_index, None);
        assert!(sfx.get_safe_prune_index().is_none());
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
        assert_eq!(sfx.messages.len(), 29);

        assert_eq!(sfx.meta.prune_index, Some(24));
        assert!(sfx.get_safe_prune_index().is_some());

        let prune_index = sfx.get_safe_prune_index().unwrap();
        assert_eq!(prune_index, 24);

        // prune suffix
        let result = sfx.prune_till_index(24).unwrap();
        // new length of suffix after pruning.
        assert_eq!(sfx.messages.len(), 5);
        assert_eq!(result.len(), 24); // result.len() + sfx.messages.len() = 30
        assert_eq!(sfx.meta.head, 25);
        assert_eq!(sfx.meta.prune_index, None);
    }

    #[test]
    fn test_is_not_ready_when_prune_index_is_below_prune_start_threshold() {
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

        assert_eq!(sfx.messages.len(), 30);

        let filtered_vec = filtered_versions_vec(0..20, None);
        filtered_vec.iter().for_each(|&vers| {
            sfx.update_decision(vers, vers + 30).unwrap();
        });

        assert_eq!(sfx.meta.prune_index, Some(18)); // because decisions are made only till version 19, index 18
        assert!(sfx.meta.prune_index.lt(&sfx.meta.prune_start_threshold)); // Prune index is below the start threshold for prune checks
        assert!(sfx.get_safe_prune_index().is_none()); // returns none as the prune_index is below the prune_start_threshold.
    }

    #[test]
    fn test_is_ready_when_prune_index_is_above_prune_start_threshold() {
        let mut sfx: Suffix<MockSuffixItemMessage> = Suffix::with_config(SuffixConfig {
            capacity: 30,
            prune_start_threshold: Some(10),
            ..Default::default()
        });

        // insert suffix items
        let ignore_vec: Vec<u64> = vec![22, 26, 27, 29];

        let filtered_vec = filtered_versions_vec(0..31, Some(ignore_vec));

        filtered_vec.iter().for_each(|&vers| {
            sfx.insert(vers, create_mock_candidate_message(vers)).unwrap();
        });

        assert_eq!(sfx.messages.len(), 30);

        filtered_vec.iter().for_each(|&vers| {
            sfx.update_decision(vers, vers + 30).unwrap();
        });

        assert_eq!(sfx.meta.prune_index, Some(29)); // prune_index updated till version 21 (index 20)
        assert_eq!(sfx.get_safe_prune_index(), Some(29));
        let _ = sfx.prune_till_index(sfx.meta.prune_index.unwrap());
        assert_eq!(sfx.messages.len(), 1);

        // assert_eq!(sfx.messages[0].unwrap().item_ver, 21);
        assert_eq!(sfx.meta.head, 30);
    }
    #[test]
    fn test_prune_after_min_size_after_prune_check_pass() {
        let mut sfx: Suffix<MockSuffixItemMessage> = Suffix::with_config(SuffixConfig {
            capacity: 30,
            prune_start_threshold: Some(20),
            min_size_after_prune: Some(15),
        });

        // insert suffix items
        let ignore_vec: Vec<u64> = vec![22, 26, 27, 29];

        let filtered_vec = filtered_versions_vec(0..31, Some(ignore_vec));

        filtered_vec.iter().for_each(|&vers| {
            sfx.insert(vers, create_mock_candidate_message(vers)).unwrap();
        });

        assert_eq!(sfx.messages.len(), 30);

        let filtered_vec = filtered_versions_vec(0..31, Some(vec![22, 26, 27, 28, 29]));
        filtered_vec.iter().for_each(|&vers| {
            sfx.update_decision(vers, vers + 30).unwrap();
        });

        // Since version 28 is not decided, prune index can be updated till version 25, which is 24
        assert_eq!(sfx.meta.prune_index, Some(24));
        // assert!(sfx.meta.prune_index.le(&sfx.meta.prune_start_threshold));
        // Although prune_index moved till 24, because the min_size_after_prune is 15, and the suffix length is 30,
        // it doesn't prune till 24th index, but to an index which ensure there will be atleast 15 items in suffix left
        assert_eq!(sfx.get_safe_prune_index(), Some(14));
    }

    #[test]
    fn test_no_prune_after_min_size_after_prune_check_fail() {
        let mut sfx: Suffix<MockSuffixItemMessage> = Suffix::with_config(SuffixConfig {
            capacity: 30,
            prune_start_threshold: Some(23),
            min_size_after_prune: Some(5),
        });

        // insert suffix items
        let ignore_vec: Vec<u64> = vec![22, 26, 27, 29];

        let filtered_vec = filtered_versions_vec(0..31, Some(ignore_vec));

        filtered_vec.iter().for_each(|&vers| {
            sfx.insert(vers, create_mock_candidate_message(vers)).unwrap();
        });

        assert_eq!(sfx.messages.len(), 30);

        let ignore_vec: Vec<u64> = vec![22, 24, 26, 27, 29];
        let filtered_vec = filtered_versions_vec(0..31, Some(ignore_vec));
        filtered_vec.iter().for_each(|&vers| {
            sfx.update_decision(vers, vers + 30).unwrap();
        });

        assert_eq!(sfx.meta.prune_index, Some(22)); // prune_index updated till version 20 (index 20)
        assert!(sfx.meta.prune_index.le(&sfx.meta.prune_start_threshold));
        // As min_size_after_prune is 5, prune_index is at 20 and suffix length is 31, it is safe to remove all the entries till prune_index
        assert_eq!(sfx.get_safe_prune_index(), None);
    }

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

    #[test]
    fn test_get_non_empty_items_when_no_empty_items() {
        let list_to_scan = [Some(10_u8); 100];
        let result = get_nonempty_suffix_items(list_to_scan.iter());

        assert_eq!(result.count(), 100);
    }

    #[test]
    fn test_get_non_empty_items_with_empty_items() {
        let list_to_scan = (0..100)
            .map(|i| {
                if i % 2 == 0 {
                    return None;
                }
                Some(i)
            })
            .collect::<Vec<Option<i32>>>();
        let result = get_nonempty_suffix_items(list_to_scan.iter()).collect::<Vec<&i32>>();

        assert_eq!(result.len(), 50);
        assert_eq!(result[1], &3);

        let last_item = *result.last().unwrap();
        assert_eq!(last_item, &99);
    }
}
