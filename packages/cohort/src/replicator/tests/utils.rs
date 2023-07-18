use crate::replicator::{
    tests::test_utils::{build_test_suffix_item, BankStatemapTestCandidate},
    utils::{get_filtered_batch, get_statemap_from_suffix_items},
};

#[test]
fn test_get_filtered_batch_all_pass() {
    //Test data
    let item1 = build_test_suffix_item(10, Some(11), BankStatemapTestCandidate::create_with_statemap(1).set_safepoint(Some(1)));
    let item2 = build_test_suffix_item(12, Some(15), BankStatemapTestCandidate::create_with_statemap(2).set_safepoint(Some(1)));
    let item3 = build_test_suffix_item(13, Some(14), BankStatemapTestCandidate::create_with_statemap(1).set_safepoint(Some(2)));
    let item4 = build_test_suffix_item(16, Some(18), BankStatemapTestCandidate::create_with_statemap(1).set_safepoint(Some(2)));
    let item5 = build_test_suffix_item(17, Some(20), BankStatemapTestCandidate::create_with_statemap(1).set_safepoint(Some(2)));
    let suffix_item = vec![&item1, &item2, &item3, &item4, &item5];

    let result = get_filtered_batch(suffix_item.into_iter());

    assert_eq!(result.count(), 5);
}

#[test]
fn test_get_filtered_batch_stop_on_undecided() {
    //Test data
    let item1 = build_test_suffix_item(10, Some(11), BankStatemapTestCandidate::create_with_statemap(1).set_safepoint(Some(1)));
    // Undecided item.
    let item2 = build_test_suffix_item(12, None, BankStatemapTestCandidate::create_with_statemap(2).set_safepoint(Some(1)));
    let item3 = build_test_suffix_item(13, Some(14), BankStatemapTestCandidate::create_with_statemap(1).set_safepoint(Some(2)));
    let suffix_item = vec![&item1, &item2, &item3];

    let result = get_filtered_batch(suffix_item.into_iter());

    assert_eq!(result.count(), 1);
}

#[test]
fn test_get_filtered_batch_remove_items_no_safepoint() {
    //Test data
    let item1 = build_test_suffix_item(10, Some(11), BankStatemapTestCandidate::create_with_statemap(1).set_safepoint(Some(1)));
    let item2 = build_test_suffix_item(12, Some(15), BankStatemapTestCandidate::create_with_statemap(1)); // This item should be removed as safepoint is None
    let item3 = build_test_suffix_item(13, Some(14), BankStatemapTestCandidate::create_with_statemap(1).set_safepoint(Some(2)));
    let suffix_item = vec![&item1, &item2, &item3];

    let mut result = get_filtered_batch(suffix_item.into_iter());

    assert_eq!(result.next().unwrap().item_ver, 10);
    assert_eq!(result.next().unwrap().item_ver, 13);
    assert!(result.next().is_none());
}

#[test]
fn test_get_filtered_batch_remove_items_no_statemap() {
    //Test data

    // item1 doesn't have statemap, and therefore shouldn't be in the result
    let item1 = build_test_suffix_item(10, Some(11), BankStatemapTestCandidate::default().set_safepoint(Some(1)));
    let item2 = build_test_suffix_item(12, Some(15), BankStatemapTestCandidate::create_with_statemap(2).set_safepoint(Some(1)));
    let item3 = build_test_suffix_item(13, Some(14), BankStatemapTestCandidate::create_with_statemap(3).set_safepoint(Some(2)));
    let item4 = build_test_suffix_item(16, Some(18), BankStatemapTestCandidate::create_with_statemap(1).set_safepoint(Some(2)));
    let suffix_item = vec![&item1, &item2, &item3, &item4];

    let mut result = get_filtered_batch(suffix_item.into_iter());

    assert_eq!(result.next().unwrap().item_ver, 12);
    assert_eq!(result.last().unwrap().item_ver, 16);
}
#[test]
fn test_get_all_statemap_from_suffix_items() {
    //Test data

    // item1 doesn't have statemap, and therefore shouldn't be in the result
    let item1 = build_test_suffix_item(10, Some(11), BankStatemapTestCandidate::create_with_statemap(3).set_safepoint(Some(1)));
    let item2 = build_test_suffix_item(12, Some(15), BankStatemapTestCandidate::create_with_statemap(1).set_safepoint(Some(1)));
    let item3 = build_test_suffix_item(13, Some(14), BankStatemapTestCandidate::create_with_statemap(5).set_safepoint(Some(2)));
    let item4 = build_test_suffix_item(16, Some(18), BankStatemapTestCandidate::create_with_statemap(1).set_safepoint(Some(2)));
    let suffix_item = vec![&item1, &item2, &item3, &item4];

    let result = get_filtered_batch(suffix_item.into_iter());

    let state_map_batch = get_statemap_from_suffix_items(result);
    assert_eq!(state_map_batch[0].1.len(), 3); // three items in statemap for version 10
    assert_eq!(state_map_batch[0].0, 10);
    assert_eq!(state_map_batch[2].0, 13);
    assert_eq!(state_map_batch[3].0, 16);
    assert_eq!(state_map_batch.last().unwrap().0, 16);
}

#[test]
fn test_get_statemap_from_suffix_items_no_statemaps() {
    //Test data

    // item1 doesn't have statemap, and therefore shouldn't be in the result
    let item1 = build_test_suffix_item(10, Some(11), BankStatemapTestCandidate::default().set_safepoint(Some(1)));
    let item2 = build_test_suffix_item(12, Some(15), BankStatemapTestCandidate::default().set_safepoint(Some(1)));
    let suffix_item = vec![&item1, &item2];

    // let result = get_filtered_batch(suffix_item.into_iter());

    let state_map_batch = get_statemap_from_suffix_items(suffix_item.into_iter());
    assert_eq!(state_map_batch.len(), 2);
    assert!(state_map_batch[0].1.is_empty());
    assert!(state_map_batch[1].1.is_empty());
}
