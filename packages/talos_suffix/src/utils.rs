/// Retrives all the items that are valid and not None.
pub fn get_nonempty_suffix_items<'a, T: 'a>(items: impl Iterator<Item = &'a Option<T>> + 'a) -> impl Iterator<Item = &'a T> + 'a {
    items.into_iter().flatten()
}

pub fn _get_percentage_of(number: f64, percentage: u64) -> f64 {
    (number * (percentage as f64 / 100_f64)).round()
}
