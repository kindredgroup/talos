mod core;
mod models;
mod services;
mod suffix;
mod talos_cohort_replicator;
pub mod utils;

pub use crate::core::{ReplicatorInstallStatus, ReplicatorInstaller, ReplicatorSnapshotProvider, StatemapItem};
pub use talos_cohort_replicator::{talos_cohort_replicator, CohortReplicatorConfig};
#[cfg(test)]
mod tests;
