/// Retrives all the items that are valid and not None.
pub fn get_nonempty_suffix_items<'a, T: 'a>(items: impl Iterator<Item = &'a Option<T>> + 'a) -> impl Iterator<Item = &'a T> + 'a {
    let k = items.filter_map(|x| x.is_some().then(|| x.as_ref().unwrap()));
    k
}
