use talos_suffix::SuffixItem;
use time::OffsetDateTime;

use crate::{
    core::StatemapItem,
    events::{EventTimingsMap, ReplicatorEvents},
    suffix::ReplicatorSuffixItemTrait,
};

/// Get all contiguous items decided irrespective of committed or aborted.
pub fn get_filtered_batch<'a, T: ReplicatorSuffixItemTrait + 'a>(messages: impl Iterator<Item = &'a SuffixItem<T>>) -> impl Iterator<Item = &'a SuffixItem<T>> {
    messages.into_iter().take_while(|&m| m.is_decided)
    // select only the messages that have safepoint i.e committed messages and select only the messages that have statemap.
    // .filter(|&m| m.item.get_safepoint().is_some() && m.item.get_statemap().is_some())
    // .filter(|&m| m.item.get_statemap().is_some()) // select only the messages that have statemap.
}

/// Takes an iterator of suffix items and returns a vector of versions and statemaps
// TODO: Instrument with span
pub fn get_statemap_from_suffix_items<'a, T: ReplicatorSuffixItemTrait + 'a>(
    messages: impl Iterator<Item = &'a SuffixItem<T>>,
) -> Vec<(u64, Vec<StatemapItem>, EventTimingsMap)> {
    messages.into_iter().fold(vec![], |mut acc, m| {
        // Record the time when the statemap item is picked.
        let mut event_timings = m.item.get_event_timings();
        event_timings.insert(ReplicatorEvents::StatemapPicked, OffsetDateTime::now_utc().unix_timestamp_nanos());
        //  aborts
        if m.item.get_safepoint().is_none() {
            acc.push((m.item_ver, vec![], event_timings));
            return acc;
        }

        // commits
        match m.item.get_statemap().as_ref() {
            //  when there is statemap
            Some(sm_items) => {
                let state_maps_to_append = sm_items.iter().map(|sm| {
                    let key = sm.keys().next().unwrap().to_string();
                    let payload = sm.get(&key).unwrap().clone();
                    StatemapItem {
                        action: key,
                        payload,
                        version: m.item_ver,
                        safepoint: *m.item.get_safepoint(),
                    }
                });
                acc.push((m.item_ver, state_maps_to_append.collect::<Vec<StatemapItem>>(), event_timings));
                acc
            }
            // when there is no statemap
            None => {
                // Empty statemap items are send for installs anyways to update the snapshot
                acc.push((m.item_ver, vec![], event_timings));
                acc
            }
        }
    })
}
