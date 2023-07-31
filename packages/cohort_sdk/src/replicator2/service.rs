use talos_certifier::{ports::MessageReciever, ChannelMessage};

use crate::replicator::core::ReplicatorInstaller;

use super::cohort_replicator::CohortReplicator;
use super::model::{InstallOutcome, StateMapWithVersion};

pub struct ReplicatorService2 {}

impl ReplicatorService2 {
    /// Infinite loop which receives StateMaps through mpsc channel and
    /// passes them to "installer".
    pub async fn start_installer<T>(
        mut rx: tokio::sync::mpsc::Receiver<(StateMapWithVersion, i128)>,
        tx_response: tokio::sync::mpsc::Sender<InstallOutcome>,
        mut statemap_installer: T,
    ) -> Result<(), String>
    where
        T: ReplicatorInstaller,
    {
        loop {
            let received = rx.recv().await;
            if let Some((item, _decided_at)) = received {
                let result = statemap_installer.install(item.statemap, Some(item.version)).await;
                let response = if let Err(error) = result {
                    log::error!("installer service: install error: {:?}, {}", error, item.version);

                    InstallOutcome::Error {
                        version: item.version,
                        started_at: 0,
                        finished_at: 0,
                        error,
                    }
                } else {
                    InstallOutcome::Success {
                        version: item.version,
                        started_at: 0,
                        finished_at: 0,
                    }
                };

                let _ = tx_response.send(response).await;
            }
        }
    }

    pub async fn start_replicator<M>(
        mut replicator: CohortReplicator<M>,
        statemaps_tx: tokio::sync::mpsc::Sender<(StateMapWithVersion, i128)>,
        mut rx_install_response: tokio::sync::mpsc::Receiver<InstallOutcome>,
    ) -> Result<(), String>
    where
        M: MessageReciever<Message = ChannelMessage> + Send + Sync,
    {
        loop {
            tokio::select! {
                is_decision = replicator.receive() => {
                    if !is_decision {
                        continue
                    }

                    let mut recent_version = 0;
                    loop {
                        let next = replicator.get_next_statemap();
                        if next.is_none() {
                            break;
                        }

                        let (statemap, decided_at) = next
                            .map(|(statemap, version, decided_at)| (StateMapWithVersion { statemap, version }, decided_at))
                            .unwrap();

                        let v = statemap.version;
                        if recent_version == statemap.version {
                            log::warn!(" will not schedule the same statemap ver({})", v);
                            break;
                        } else {
                            recent_version = statemap.version;
                        }

                        // m_f_send.clock_start();
                        let rslt_send = statemaps_tx.send((statemap, decided_at.unwrap_or(0))).await.map_err(|e| format!("Error: {}", e));
                        // m_f_send.clock_end();

                        if let Err(e) = rslt_send {
                            log::warn!("Unable to send statemap to installer. {}", e);
                        } else {
                            log::debug!(" scheduled statemap to install ver({})", v);
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
