// $coverage:ignore-start
use std::fmt::Debug;

use crate::replicator::{
    core::{Replicator, ReplicatorCandidate, ReplicatorChannel, StatemapItem},
    suffix::ReplicatorSuffixTrait,
};

use log::{debug, info};
use talos_certifier::{ports::MessageReciever, ChannelMessage};
use tokio::sync::mpsc;

pub async fn replicator_service<S, M>(
    statemaps_tx: mpsc::Sender<(Vec<StatemapItem>, Option<u64>)>,
    mut replicator_rx: mpsc::Receiver<ReplicatorChannel>,
    mut replicator: Replicator<ReplicatorCandidate, S, M>,
) -> Result<(), String>
where
    S: ReplicatorSuffixTrait<ReplicatorCandidate> + Debug,
    M: MessageReciever<Message = ChannelMessage> + Send + Sync,
{
    info!("Starting Replicator Service.... ");

    loop {
        tokio::select! {
        // 1. Consume message.
        res = replicator.receiver.consume_message() => {
            if let Ok(Some(msg)) = res {

                // 2. Add/update to suffix.
                match msg {
                    // 2.1 For CM - Install messages on the version
                    ChannelMessage::Candidate( message) => {
                        let version = message.version;
                        replicator.process_consumer_message(version, message.into()).await;
                    },
                    // 2.2 For DM - Update the decision with outcome + safepoint.
                    ChannelMessage::Decision(decision_version, decision_message) => {
                        replicator.process_decision_message(decision_version, decision_message).await;


                        // Get a batch of remaining versions with their statemaps to install.
                        let statemaps_batch = replicator.generate_statemap_batch();

                        if !statemaps_batch.is_empty() {
                            for (vers, statemaps_to_install) in statemaps_batch {
                                // send for install.
                                statemaps_tx.send((statemaps_to_install, Some(vers))).await.unwrap();

                            };
                        }


                    },
                }
            }
        }
        res = replicator_rx.recv() => {
            if let Some(result) = res {
                match result {
                    // 4. Remove the versions if installations are complete.
                    ReplicatorChannel::InstallationSuccess(vers) => {

                        let version = vers.last().unwrap().to_owned();
                        debug!("Installated successfully till version={version:?}");
                        // Mark the suffix item as installed.
                        replicator.suffix.set_item_installed(version);

                        // if all prior items are installed, then update the prune vers
                        replicator.suffix.update_prune_index(version);


                        // Prune suffix and update suffix head.
                        if replicator.suffix.get_suffix_meta().prune_index >= replicator.suffix.get_suffix_meta().prune_start_threshold {
                            replicator.suffix.prune_till_version(version).unwrap();
                        }

                        // commit the offset
                        replicator.receiver.commit(version).await.unwrap();
                    }
                }
            }
        }

        }
    }
}

// $coverage:ignore-end
