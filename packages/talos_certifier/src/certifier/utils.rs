use ahash::AHashMap;

pub fn convert_vec_to_hashmap(v: Vec<(&str, u64)>) -> AHashMap<String, u64> {
    v.into_iter().map(|(k, v)| (k.to_owned(), v)).collect()
}
