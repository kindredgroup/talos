pub mod core;
pub mod errors;
mod suffix;
mod utils;

pub use crate::core::{SuffixItem, SuffixTrait};
pub use suffix::Suffix;
pub use utils::get_nonempty_suffix_items;

// Unit Tests
#[cfg(test)]
mod tests;
