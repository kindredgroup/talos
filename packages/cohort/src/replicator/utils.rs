use talos_suffix::SuffixItem;

use super::{core::StatemapItem, suffix::ReplicatorSuffixItemTrait};

pub fn get_filtered_batch<'a, T: ReplicatorSuffixItemTrait + 'a>(messages: impl Iterator<Item = &'a SuffixItem<T>>) -> impl Iterator<Item = &'a SuffixItem<T>> {
    messages
        .into_iter()
        .take_while(|&m| m.is_decided)
        .filter(|&m| m.item.get_safepoint().is_some()) // select only the messages that have safepoint i.e committed messages
        .filter(|&m| m.item.get_statemap().is_some()) // select only the messages that have statemap.
}

pub fn get_statemap_from_suffix_items<'a, T: ReplicatorSuffixItemTrait + 'a>(messages: impl Iterator<Item = &'a SuffixItem<T>>) -> Vec<StatemapItem> {
    messages.into_iter().fold(vec![], |mut acc, m| match m.item.get_statemap().as_ref() {
        Some(sm_items) => {
            let state_maps_to_append = sm_items.iter().map(|sm| {
                let key = sm.keys().next().unwrap().to_string();
                let payload = sm.get(&key).unwrap().clone();
                StatemapItem {
                    action: key,
                    payload,
                    version: m.item_ver,
                }
            });
            acc.extend(state_maps_to_append);
            acc
        }
        None => acc,
    })
}
