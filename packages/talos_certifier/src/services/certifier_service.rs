use std::sync::Arc;
use std::{collections::VecDeque, sync::atomic::AtomicI64};

use async_trait::async_trait;
use log::{debug, info, trace};
use talos_suffix::core::convert_u64_to_usize;
use tokio::sync::mpsc;

use crate::{
    core::{DecisionOutboxChannelMessage, ServiceResult, System, SystemService},
    errors::{SystemErrorType, SystemServiceError, SystemServiceErrorKind},
    model::{CandidateMessage, DecisionMessage},
    Certifier, ChannelMessage, SystemMessage,
};

fn index_from_head(head: u64, version: u64) -> Option<usize> {
    if version < head {
        None
    } else {
        Some(convert_u64_to_usize(version - head).ok()?)
    }
}
pub struct CertifierService {
    // pub suffix: Suffix<CandidateMessage>,
    pub certifier: Certifier,
    pub system: System,
    pub message_channel_rx: mpsc::Receiver<ChannelMessage>,
    pub commit_offset: Arc<AtomicI64>,
    pub decision_outbox_tx: mpsc::Sender<DecisionOutboxChannelMessage>,
    pub decision_vec: VecDeque<Option<bool>>,
    pub current_head: u64,
    pub prune_vers: u64,
}

impl CertifierService {
    pub fn new(
        message_channel_rx: mpsc::Receiver<ChannelMessage>,
        decision_outbox_tx: mpsc::Sender<DecisionOutboxChannelMessage>,
        commit_offset: Arc<AtomicI64>,
        system: System,
    ) -> Self {
        let certifier = Certifier::new();
        // let suffix = Suffix::new(100_000);

        let vec_decisions: Vec<Option<bool>> = vec![None; 10_000];

        Self {
            // suffix,
            certifier,
            system,
            message_channel_rx,
            decision_outbox_tx,
            commit_offset,
            decision_vec: VecDeque::from(vec_decisions),
            current_head: 0,
            prune_vers: 0,
        }
    }

    // fn get_conflict_candidates(&mut self, outcome: &Outcome) -> Option<CandidateMessage> {
    //     match outcome {
    //         Outcome::Aborted { version, discord: _ } => {
    //             if version.is_some() {
    //                 if let Ok(Some(msg)) = self.suffix.get(version.unwrap()) {
    //                     return Some(msg.item);
    //                 }
    //             }
    //             None
    //         }
    //         _ => None,
    //     }
    // }

    /// Process CandidateMessage to provide the DecisionMessage
    ///
    /// * Inserts the message into suffix.
    /// * Certify the message.
    /// * Create the DecisionMessage from the certification outcome.
    ///
    pub(crate) fn process_candidate_message_outcome(&mut self, message: &CandidateMessage) -> ServiceResult<DecisionMessage> {
        // Insert into Suffix
        // self.suffix.insert(message.version, message.clone()).map_err(CertificationError::SuffixError)?;
        // let suffix_head = self.suffix.meta.head;
        if self.current_head == 0 {
            self.current_head = message.version;
        }

        // Get certifier outcome
        let outcome = self
            .certifier
            .certify_transaction(self.current_head, message.convert_into_certifier_candidate(message.version));

        // If abort get the conflict record from suffix
        // let conflict_candidate: Option<CandidateMessage> = self.get_conflict_candidates(&outcome);

        // Create the Decision Message
        let decision = DecisionMessage::new(message, outcome.clone(), self.current_head);
        info!("Decision message after certifying is {decision:#?} and \n outcome is {outcome:#?}");
        Ok(decision)
    }

    pub(crate) async fn process_candidate(&mut self, message: &CandidateMessage) -> ServiceResult {
        let decision_message = self.process_candidate_message_outcome(message)?;

        self.decision_outbox_tx
            .send(DecisionOutboxChannelMessage::Decision(decision_message.clone()))
            .await
            .map_err(|e| SystemServiceError {
                kind: SystemServiceErrorKind::SystemError(SystemErrorType::Channel),
                data: Some(format!("{:?}", decision_message)),
                reason: e.to_string(),
                service: "Certifier Service".to_string(),
            })?;

        Ok(())
    }

    pub(crate) async fn process_decision(&mut self, version: u64, decision_message: &DecisionMessage) -> ServiceResult {
        // update the decision in suffix
        debug!("Version {} and Decision Message {:?} ", version, decision_message);

        // let decision_version_index = index_from_head(self.current_head, version).unwrap();

        let candidate_message_version = decision_message.version;

        // version below head.
        if candidate_message_version < self.current_head {
            return Ok(());
        }

        info!(
            "Head is {} and Decision version=({version}) for candidate version=({candidate_message_version})",
            self.current_head
        );
        let candidate_version_index = index_from_head(self.current_head, candidate_message_version).unwrap();

        // if version is greater than the length of the vec
        if self.decision_vec.len() < candidate_version_index {
            self.decision_vec.resize_with(candidate_version_index + 1, || None);
        }

        // update the decision
        self.decision_vec.insert(candidate_version_index, Some(true));

        // commit the offset if all prior suffix items have been decided?
        // let current_commit_offset: u64 = self.commit_offset.load(std::sync::atomic::Ordering::Relaxed).try_into().unwrap();

        // finds if all the items in the DQ prioir to the current one are decided.
        let result = if candidate_message_version > self.prune_vers {
            let prune_vers_index = index_from_head(self.current_head, self.prune_vers).unwrap_or(0);
            self.decision_vec
                .range(prune_vers_index..candidate_version_index)
                .filter_map(|x| x.is_some().then(|| x.as_ref().unwrap()))
                .all(|x| *x)
        } else {
            let slice = self.decision_vec.make_contiguous().split_at(candidate_version_index).0;
            slice.iter().filter_map(|x| x.is_some().then(|| x.as_ref().unwrap())).all(|x| *x)
        };

        if result {
            //commit the offset if all prioir items are decided.
            self.commit_offset.store(version.try_into().unwrap(), std::sync::atomic::Ordering::Relaxed);
            //update the prune version.
            self.prune_vers = candidate_message_version;

            //* prune if additional conditions are met.
            //** update the head on prune.
            //** prune certifier read and write set as well if additional conditions are met
        }

        // Reserve space if version is beyond the suffix capacity
        //
        // Applicable in scenarios where certifier starts from a committed version
        // if version > self.suffix.messages.capacity().try_into().unwrap() {
        //     self.suffix.reserve_space_if_required(version).map_err(CertificationError::SuffixError)?;
        // }

        // self.suffix
        //     .update_decision_suffix_item(decision_message.version, version)
        //     .map_err(CertificationError::SuffixError)?;

        // prune suffix if required?
        // self.suffix.prune();
        // remove sets from certifier if pruning?

        // commit the offset if all prior suffix items have been decided?
        // if self.suffix.are_prior_items_decided(decision_message.version) {
        //     self.suffix.update_prune_vers(Some(decision_message.version));

        //     self.commit_offset.store(version.try_into().unwrap(), std::sync::atomic::Ordering::Relaxed);
        // }

        Ok(())
    }
}

#[async_trait]
impl SystemService for CertifierService {
    async fn shutdown_service(&mut self) {
        debug!("Shutting down Certifier!!!");

        self.system.is_shutdown = true;
        // drop(self.decision_publisher);
        info!("Certifier Service shutdown completed!");
    }
    fn is_shutdown(&self) -> bool {
        self.system.is_shutdown
    }

    async fn update_shutdown_flag(&mut self, flag: bool) {
        info!("flag {}", flag);
        self.system.is_shutdown = flag;
    }
    async fn health_check(&self) -> bool {
        true
    }

    async fn run(&mut self) -> Result<(), SystemServiceError> {
        let mut system_channel_rx = self.system.system_notifier.subscribe();

        // while !self.is_shutdown() {
        tokio::select! {
           channel_msg =  self.message_channel_rx.recv() =>  {
                match channel_msg {
                    Some(ChannelMessage::Candidate( message)) => {
                         self.process_candidate(&message).await?

                    },

                    Some(ChannelMessage::Decision(version, decision_message)) => {
                        trace!("Processing decision with {version} ");
                        self.process_decision(version, &decision_message).await?
                    },

                    None => (),
                }
            }
            // ** Received System Messages (shutdown/healthcheck).
            msg = system_channel_rx.recv() => {
                    let message = msg.unwrap();

                    match message {
                        SystemMessage::Shutdown => {
                            info!("[CERTIFIER] Shutdown received");
                            self.shutdown_service().await;
                        },
                        SystemMessage::HealthCheck => {
                            // info!("Health Check message received <3 <3 <3");
                            let is_healthy = self.health_check().await;
                            self.system.system_notifier.send(SystemMessage::HealthCheckStatus { service: "CERTIFIER_SERVICE", healthy: is_healthy },).unwrap();
                        },
                        _ => ()
                }

            }


        }

        Ok(())
    }
}
