// $coverage:ignore-start
use std::{fmt::Debug, io::Error, sync::Arc, time::Duration};

use crate::{
    state::postgres::{data_access::PostgresApi, database::Database},
    tx_batch_executor::BatchExecutor,
};

use super::{
    core::{Replicator, ReplicatorCandidate, StatemapItem},
    suffix::ReplicatorSuffixTrait,
};
use log::info;
use talos_certifier::{ports::MessageReciever, ChannelMessage};

async fn statemap_install_handler(sm: Vec<StatemapItem>, db: Arc<Database>, version: Option<u64>) -> Result<bool, Error> {
    info!("Last version ... {:#?} ", version);
    info!("Original statemaps received ... {:#?} ", sm);

    let mut manual_tx_api = PostgresApi { client: db.get().await };

    let result = BatchExecutor::execute(&mut manual_tx_api, sm, version).await;

    info!("Result on executing the statmaps is ... {result:?}");

    Ok(result.is_ok())
}

pub struct ReplicatorStatemapInstaller {
    pub db: Arc<Database>,
}

impl ReplicatorStatemapInstaller {
    pub async fn install(&self, sm: Vec<StatemapItem>, version: Option<u64>) -> Result<bool, Error> {
        let db = Arc::clone(&self.db);
        statemap_install_handler(sm, db, version).await
    }
}

pub async fn run_talos_replicator<
    S,
    M,
    //  F, Fut
>(
    replicator: &mut Replicator<ReplicatorCandidate, S, M>,
    // install_statemaps: F,
    statemap_installer: ReplicatorStatemapInstaller,
) where
    S: ReplicatorSuffixTrait<ReplicatorCandidate> + Debug,
    M: MessageReciever<Message = ChannelMessage> + Send + Sync,
    // Fut: Future<Output = Result<bool, Error>>,
    // F: for<'a> Fn(Vec<StatemapItem>, &'a Option<u64>) -> Fut,
{
    info!("Going to consume the message.... ");
    let mut interval = tokio::time::interval(Duration::from_millis(10));

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

                        },
                    }
                }
            }
            // 3. At set interval send the batch to be installed.
            //  3.1 Derive the new snapshot.
            //  3.2 Create batch of valid statemap instructions.
            //      (a) Select only the messages that have safepoint (i.e candidate messages with committed decisions).
            //      (b) Select only the messages that have statemap.
            //      (c) Send it to the state manager to do the updates.
            _ = interval.tick() => {

                if let (Some(statemap_batch), version_option) = replicator.generate_statemap_batch() {
                    if version_option.is_some() {

                        info!("Statemap batch in replicator_service is ={statemap_batch:?}");
                        // let version = statemap_batch.iter().last().unwrap().version;
                        // Call fn to install statemaps in batch amd update the snapshot
                        let version = version_option.unwrap();

                        let result = statemap_installer.install(statemap_batch, version_option).await;

                        info!("Installation result ={result:?}");

                        // 4. Remove the versions if installations are complete.
                        if let Ok(res) = result {
                            if res {

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
}
// $coverage:ignore-end
