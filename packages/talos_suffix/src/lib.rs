pub mod core;
pub mod errors;
mod suffix;
mod utils;

pub use crate::core::{SuffixItem, SuffixTrait};
pub use suffix::Suffix;

// Unit Tests
#[cfg(test)]
mod tests;
