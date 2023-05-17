use std::{collections::HashMap, sync::Arc};

use log::info;
use tokio::sync::Mutex;

use super::core::ReplicatorStatisticsItem;

#[derive(Debug, Default)]
pub struct PrintStatistics {
    pub total_count: u64,
    pub install_success_count: Option<u64>,
    pub install_fail_count: Option<u64>,
    pub commit_decisions_count: Option<u64>,
    pub abort_decisions_count: Option<u64>,
}

pub async fn generate_statistics(stats: Arc<Mutex<HashMap<u64, ReplicatorStatisticsItem>>>) {
    let stats_obj = stats.lock().await;

    let mut final_statistics = PrintStatistics {
        total_count: stats_obj.len() as u64,
        ..Default::default()
    };

    final_statistics.install_fail_count = Some(
        stats_obj
            .clone()
            .into_values()
            .filter(|i| i.is_statemap_install_success.is_some() && !i.is_statemap_install_success.unwrap())
            .count() as u64,
    );

    final_statistics.install_success_count = Some(final_statistics.total_count - final_statistics.install_fail_count.unwrap());

    final_statistics.commit_decisions_count = Some(
        stats_obj
            .clone()
            .into_values()
            .filter(|i| i.is_committed_decision.is_some() && i.is_committed_decision.unwrap())
            .count() as u64,
    );

    final_statistics.abort_decisions_count = Some(final_statistics.total_count - final_statistics.commit_decisions_count.unwrap());

    info!("Printing final stats");
    info!("+-----------------------------------------------------------------------------------------------------------------------");
    info!("+ Total | Commits | Aborts | Install Success | Install Fails ");
    info!("+-----------------------------------------------------------------------------------------------------------------------");
    info!(
        "+ {:5?} | {:7?} | {:6?} | {:15?} | {:13?}",
        final_statistics.total_count,
        final_statistics.commit_decisions_count.unwrap(),
        final_statistics.abort_decisions_count.unwrap(),
        final_statistics.install_success_count.unwrap(),
        final_statistics.install_fail_count.unwrap()
    );
    info!("+-----------------------------------------------------------------------------------------------------------------------");
}
