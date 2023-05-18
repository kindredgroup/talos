use std::{collections::HashMap, sync::Arc};

use log::info;
use tokio::sync::Mutex;

use super::core::ReplicatorStatisticsItem;

pub async fn generate_statistics(stats: Arc<Mutex<HashMap<u64, ReplicatorStatisticsItem>>>) {
    let stats_obj = stats.lock().await;

    let total_count = stats_obj.len() as u64;

    // Get count of failed installations.
    let install_fail_count = Some(
        stats_obj
            .clone()
            .into_values()
            .filter(|i| i.is_statemap_install_success.is_some() && !i.is_statemap_install_success.unwrap())
            .count() as u64,
    );

    //  Get count of successful installations
    let install_success_count = Some(total_count - install_fail_count.unwrap());

    // Get count of commit decisions.
    let commit_decisions_count = Some(
        stats_obj
            .clone()
            .into_values()
            .filter(|i| i.is_committed_decision.is_some() && i.is_committed_decision.unwrap())
            .count() as u64,
    );

    // Get count of abort decisions.
    let abort_decisions_count = Some(total_count - commit_decisions_count.unwrap());

    // Get count of txns that had installation retries.
    let retries = Some(stats_obj.clone().into_values().filter(|i| i.statemap_install_retries > 0).count() as u64);

    let total_installation_time = stats_obj
        .clone()
        .into_values()
        .filter(|i| i.is_statemap_install_success.is_some() && i.is_statemap_install_success.unwrap())
        .fold(0_u128, |acc, i| acc + i.statemap_install_time.unwrap());

    // Get average installation time
    let average_install_time = if install_success_count.is_some() {
        Some(total_installation_time as f64 / (install_success_count.unwrap() as f64) / 1_000_000_f64)
    } else {
        None
    };

    // Get count of successfully snapshot only installations.
    let install_abort_safepoint_only = Some(
        stats_obj
            .clone()
            .into_values()
            .filter(|i| i.is_statemap_install_success.unwrap_or_default())
            .filter(|i| !i.is_committed_decision.unwrap_or_default())
            // .filter(|i| i.statemap_batch_size.unwrap_or_default())
            .count() as u64,
    );

    // Get count of successfully installations for commit decisions with no statemap.
    let install_commits_no_statemap = Some(
        stats_obj
            .clone()
            .into_values()
            .filter(|i| i.is_statemap_install_success.unwrap_or_default())
            .filter(|i| i.is_committed_decision.unwrap_or_default())
            .filter(|i| i.statemap_batch_size.unwrap_or_default() == 0)
            .count() as u64,
    );

    // Get count of successfully installations for commit decisions with some statemap.
    let install_commits_with_statemap = Some(
        stats_obj
            .clone()
            .into_values()
            .filter(|i| i.is_statemap_install_success.unwrap_or_default())
            .filter(|i| i.is_committed_decision.unwrap_or_default())
            .filter(|i| i.statemap_batch_size.unwrap_or_default() > 0)
            .count() as u64,
    );

    info!("+-----------------------------------------------------------------------------------------------------------------------");
    info!("+                                    R E P L I C A T O R   S T A T S ");
    info!("+-----------------------------------------------------------------------------------------------------------------------");
    info!("+ Counts: ");
    info!("+-----------------------------------------------------------------------------------------------------------------------");
    info!("+ Total | Commits | Aborts | Install Success | Install Fails | Install Retries ");
    info!("+-----------------------------------------------------------------------------------------------------------------------");
    info!(
        "+ {:5?} | {:7?} | {:6?} | {:15?} | {:13?} | {:13?}",
        total_count,
        commit_decisions_count.unwrap(),
        abort_decisions_count.unwrap(),
        install_success_count.unwrap(),
        install_fail_count.unwrap(),
        retries.unwrap_or_default(),
    );
    info!("+-----------------------------------------------------------------------------------------------------------------------");
    info!("+ More Stats: ");
    info!("+-----------------------------------------------------------------------------------------------------------------------");
    info!("+ Avg Install time (ms)                : {}  ", average_install_time.unwrap_or_default());
    info!(
        "+ Snapshot only installs (Aborts)      : {}  ",
        install_abort_safepoint_only.unwrap_or_default()
    );
    info!("+ Commit Installs without statemaps    : {}  ", install_commits_no_statemap.unwrap_or_default());
    info!(
        "+ Commit Installs with statemaps       : {}  ",
        install_commits_with_statemap.unwrap_or_default()
    );
    info!("+-----------------------------------------------------------------------------------------------------------------------");
}
