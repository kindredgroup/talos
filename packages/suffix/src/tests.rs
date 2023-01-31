// --- tests
#[cfg(test)]
mod suffix_tests {

    use crate::{core::SuffixTrait, Suffix};

    #[derive(Debug, Clone)]
    #[allow(dead_code)]
    struct MockSuffixItemMessage {
        xid: String,
        agent: String,
        cohort: String,
    }

    fn create_mock_candidate_message(suffixer: u64) -> MockSuffixItemMessage {
        let xid = format!("xid-{}", suffixer);
        let agent = format!("agent-{}", suffixer);
        let cohort = format!("cohort-{}", suffixer);
        MockSuffixItemMessage { xid, agent, cohort }
    }

    #[test]
    fn get_index_from_head() {
        let mut sfx = Suffix::<MockSuffixItemMessage>::new(20);

        assert_eq!(sfx.index_from_head(10).unwrap(), 10);

        sfx.meta.head = 11;
        assert_eq!(sfx.index_from_head(11).unwrap(), 0);
        assert_eq!(sfx.index_from_head(13).unwrap(), 2);
    }
    #[test]
    fn insert_version_within_dq_len() {
        let mut sfx = Suffix::new(1000);
        // Test has length of 1000
        assert_eq!(sfx.messages.len(), 1001);
        // Test the indexes are Empty
        assert!(sfx.messages[88].is_none());
        // Test inserting to a particular index

        sfx.insert(33, create_mock_candidate_message(33)).unwrap();

        let sfxi = sfx.get(33).unwrap().unwrap();

        assert_eq!(sfxi.item.xid, create_mock_candidate_message(33).xid);
    }

    // #[test]
    // fn insert_version_below_capacity() {
    //     let mut sfx = RealSuffix::new(1000);
    //     // Test inserting to a particular index which is
    //     sfx.insert(33, create_mock_candidate_message(33));

    //     let sfxi = sfx
    //         .get(33)
    //         .and_then(|x| match x {
    //             SuffixItemEnum::SuffixItem(item) => Some(item),
    //             SuffixItemEnum::Empty => None,
    //         })
    //         .unwrap();

    //     assert_eq!(sfxi.item.xid, create_mock_candidate_message(33).xid);
    // }
    #[test]
    fn insert_version_above_dq_len() {
        let mut sfx = Suffix::new(20);
        // Test inserting to a particular index which is
        sfx.insert(30, create_mock_candidate_message(30)).unwrap();
        assert_eq!(sfx.messages.len(), 31);

        let sfxi = sfx.get(30).unwrap().unwrap();
        assert_eq!(sfxi.item.xid, create_mock_candidate_message(30).xid);
    }
    #[test]
    fn check_version_safe_to_prune_when_all_prior_is_empty() {
        let ver: u64 = 20;
        let mut sfx = Suffix::new(20);
        sfx.insert(ver, create_mock_candidate_message(ver)).unwrap();

        assert!(sfx.are_prior_items_decided(ver));
    }
    #[test]
    fn check_version_safe_to_prune_when_prior_undecided() {
        let ver: u64 = 20;
        let mut sfx = Suffix::new(20);
        sfx.insert(10, create_mock_candidate_message(10)).unwrap();
        sfx.insert(ver, create_mock_candidate_message(ver)).unwrap();

        assert!(!sfx.are_prior_items_decided(ver));
    }
    #[test]
    fn check_version_safe_to_prune_when_prior_decided() {
        let mut sfx = Suffix::new(20);
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

        let mut sfx = Suffix::new(20);
        let ver_10: u64 = 10;
        let decision_ver: u64 = 23;
        sfx.insert(ver_10, create_mock_candidate_message(ver_10)).unwrap();

        let _ = sfx.update_decision(ver_10, decision_ver);

        let ver_10_message = sfx.get(ver_10).unwrap().unwrap();

        assert_eq!(ver_10_message.item_ver, ver_10);
        assert_eq!(sfx.meta.prune_vers, Some(ver_10));
        assert_eq!(ver_10_message.decision_ver.unwrap(), decision_ver);
    }

    #[test]
    fn update_decision_when_item_not_in_suffix() {
        let mut sfx = Suffix::<MockSuffixItemMessage>::new(20);
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
    fn prune_items() {
        let mut sfx = Suffix::new(20);
        let ver_10: u64 = 10;
        sfx.insert(ver_10, create_mock_candidate_message(ver_10)).unwrap();

        let ver_25: u64 = 25;
        sfx.insert(ver_25, create_mock_candidate_message(ver_25)).unwrap();

        let decision_ver: u64 = 23;
        let _ = sfx.update_decision(ver_10, decision_ver);

        let decision_ver: u64 = 29;
        let _ = sfx.update_decision(ver_25, decision_ver);

        let ver_33: u64 = 33;
        sfx.insert(ver_33, create_mock_candidate_message(ver_33)).unwrap();

        assert_eq!(sfx.messages.len(), 34);
        assert_eq!(sfx.meta.prune_vers.unwrap(), 25);

        sfx.prune().unwrap();

        assert!(sfx.meta.prune_vers.is_none());
        assert_eq!(sfx.meta.head, 33);
        assert_eq!(sfx.messages.len(), 11);

        let sfx_v33 = sfx.get(33).unwrap().unwrap();
        assert_eq!(sfx_v33.item_ver, 33);

        let decision_ver: u64 = 39;
        let result = sfx.update_decision(ver_33, decision_ver);

        assert!(result.is_ok());

        assert!(sfx.meta.prune_vers.is_some());

        sfx.prune().unwrap();

        assert_eq!(sfx.messages.len(), 11);
    }
    // #[test]
    // fn get_returns_empty() {}
    // #[test]
    // fn get_returns_correct_version() {}
}
