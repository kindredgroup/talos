use std::{sync::Arc, time::Duration};

use metrics::model::{MicroMetrics, MinMax};
use talos_certifier::{ports::MessageReciever, ChannelMessage};
use time::OffsetDateTime;

use crate::replicator::{core::ReplicatorInstaller, pg_replicator_installer::PgReplicatorStatemapInstaller};

use super::cohort_replicator::CohortReplicator;
use super::model::{InstallOutcome, StateMapWithVersion};

pub struct ReplicatorService2 {}

impl ReplicatorService2 {
    /// Infinite loop which receives StateMaps through mpsc channel and
    /// passes them to "installer".
    pub async fn start_installer(
        mut rx: tokio::sync::mpsc::Receiver<(StateMapWithVersion, i128)>,
        tx_response: tokio::sync::mpsc::Sender<InstallOutcome>,
        mut installer: PgReplicatorStatemapInstaller,
        tx_heartbeat: Arc<tokio::sync::watch::Sender<u64>>,
        metrics_print_frequency: Option<i128>,
    ) -> Result<(), String> {
        let mut metrics = MicroMetrics::new(1_000_000_000_f32, true);
        let mut m_elapsed_since_decision = MinMax::default();
        loop {
            // m_f_read.clock_start();
            let received = rx.recv().await;
            // m_f_read.clock_end();

            if let Some((item, decided_at)) = received {
                if decided_at > 0 {
                    m_elapsed_since_decision.add(OffsetDateTime::now_utc().unix_timestamp_nanos() - decided_at);
                }
                metrics.start();

                let started_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
                // m_f_install.clock_start();
                let result = installer.install(item.statemap, Some(item.version)).await;
                // m_f_install.clock_end();
                let finished_at = OffsetDateTime::now_utc().unix_timestamp_nanos();

                let elapsed = metrics.clock_end();
                if let Some(frequency) = metrics_print_frequency {
                    if elapsed >= frequency {
                        metrics.sample_end();
                        log::warn!(
                            "METRIC (installer) : {},{},{},{}",
                            m_elapsed_since_decision.count,
                            Duration::from_nanos(m_elapsed_since_decision.min as u64).as_micros(),
                            Duration::from_nanos(m_elapsed_since_decision.max as u64).as_micros(),
                            Duration::from_nanos(m_elapsed_since_decision.sum as u64).as_micros(),
                        );
                        let _ = tx_heartbeat.send(metrics.count as u64);
                        m_elapsed_since_decision.reset();
                    }
                }

                let response = if let Err(error) = result {
                    log::error!("installer service: install error: {:?}, {}", item.version, finished_at - started_at);

                    InstallOutcome::Error {
                        version: item.version,
                        started_at,
                        finished_at,
                        error,
                    }
                } else {
                    InstallOutcome::Success {
                        version: item.version,
                        started_at,
                        finished_at,
                    }
                };

                let _ = tx_response.send(response).await;
            }
        }
    }

    pub async fn start_replicator<M>(
        mut replicator: CohortReplicator<M>,
        tx: tokio::sync::mpsc::Sender<(StateMapWithVersion, i128)>,
        mut rx_install_response: tokio::sync::mpsc::Receiver<InstallOutcome>,
        metrics_print_frequency: Option<i128>,
    ) -> Result<(), String>
    where
        M: MessageReciever<Message = ChannelMessage> + Send + Sync,
    {
        let mut metric = MicroMetrics::new(1_000_000_000_f32, true);
        let mut m_f_send = MicroMetrics::new(1_000_000_f32, true);
        let mut m_f_pick = MicroMetrics::new(1_000_000_f32, true);

        loop {
            tokio::select! {
                is_decision = replicator.receive() => {
                    if !is_decision {
                        continue
                    }
                    metric.start();

                    let mut recent_version = 0;
                    loop {
                        let next = replicator.get_next_statemap();
                        if next.is_none() {
                            break;
                        }

                        m_f_pick.clock_start();
                        let (statemap, decided_at) = next
                            .map(|(statemap, version, decided_at)| (StateMapWithVersion { statemap, version }, decided_at))
                            .unwrap();
                        m_f_pick.clock_end();

                        let v = statemap.version;
                        if recent_version == statemap.version {
                            log::warn!(" will not schedule the same statemap ver({})", v);
                            break;
                        } else {
                            recent_version = statemap.version;
                        }

                        m_f_send.clock_start();
                        let rslt_send = tx.send((statemap, decided_at.unwrap_or(0))).await.map_err(|e| format!("Error: {}", e));
                        m_f_send.clock_end();

                        if let Err(e) = rslt_send {
                            log::warn!("Unable to send statemap to installer. {}", e);
                        } else {
                            log::debug!(" scheduled statemap to install ver({})", v);
                        }
                    }

                    let elapsed = metric.clock_end();
                    if let Some(frequency) = metrics_print_frequency {
                        if elapsed >= frequency {
                            metric.sample_end();
                            log::warn!(
                                "METRIC (replicator) : {}, | lookups: {}, | sends: {}, cap: {}",
                                metric.print("s"),
                                m_f_pick.print("ms"),
                                m_f_send.print("ms"),
                                tx.capacity(),
                            );

                            // replicator.commit().await;
                        }
                    }
                }

                opt_install_resp = rx_install_response.recv() => {
                    if let Some(InstallOutcome::Success { version, started_at: _started_at, finished_at: _finished_at }) = opt_install_resp {
                        // ack += 1;
                        if let Err(e) = replicator.update_suffix(version) {
                            log::warn!("Error updating suffix for version: {}. Error: {}", version, e);
                        } else {
                           // let _ = replicator.receiver.commit(version).await;
                        }
                    }
                }
            }
        }
    }
}
