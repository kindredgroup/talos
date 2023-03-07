use multimap::MultiMap;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use time::OffsetDateTime;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::agent::model::{CancelRequestChannelMessage, CertifyRequestChannelMessage};

use crate::api::{AgentConfig, CertificationResponse, TRACK_PUBLISH_LATENCY};
use crate::messaging::api::{CandidateMessage, Decision, DecisionMessage, PublisherType};

/// Structure represents client who sent the certification request.
struct WaitingClient {
    /// Time when certification request received.
    received_at: u64,
    tx_sender: Sender<CertificationResponse>,
}

impl WaitingClient {
    pub fn new(tx_sender: Sender<CertificationResponse>) -> Self {
        WaitingClient {
            received_at: OffsetDateTime::now_utc().unix_timestamp_nanos() as u64,
            tx_sender,
        }
    }
    async fn notify(&self, response: CertificationResponse, error_message: String) {
        if let Err(e) = self.tx_sender.send(response).await {
            log::error!("{}. Error: {}", error_message, e);
        };
    }
}

pub struct StateManager {
    agent_config: AgentConfig,
    publish_times: Arc<Mutex<HashMap<String, u64>>>,
}

impl StateManager {
    pub fn new(agent_config: AgentConfig, publish_times: Arc<Mutex<HashMap<String, u64>>>) -> StateManager {
        StateManager { agent_config, publish_times }
    }

    pub async fn run(
        &self,
        mut rx_certify: Receiver<CertifyRequestChannelMessage>,
        mut rx_cancel: Receiver<CancelRequestChannelMessage>,
        mut rx_decision: Receiver<DecisionMessage>,
        publisher: Arc<Box<PublisherType>>,
    ) {
        let mut state: MultiMap<String, WaitingClient> = MultiMap::new();
        loop {
            tokio::select! {
                rslt_request_msg = rx_certify.recv() => {
                    self.handle_candidate(rslt_request_msg, publisher.clone(), &mut state).await;
                }

                rslt_cancel_request_msg = rx_cancel.recv() => {
                    Self::handle_cancellation(rslt_cancel_request_msg, &mut state);
                }

                rslt_decision_msg = rx_decision.recv() => {
                    Self::handle_decision(rslt_decision_msg, &mut state).await;
                }
            }
        }
    }

    /// Passes candidate to kafka publisher and records it in the internal state.
    /// The publishing action is done asynchronously.
    async fn handle_candidate(
        &self,
        opt_candidate: Option<CertifyRequestChannelMessage>,
        publisher: Arc<Box<PublisherType>>,
        state: &mut MultiMap<String, WaitingClient>,
    ) {
        if let Some(request_msg) = opt_candidate {
            state.insert(request_msg.request.candidate.xid.clone(), WaitingClient::new(request_msg.tx_answer));

            let msg = CandidateMessage::new(
                self.agent_config.agent.clone(),
                self.agent_config.cohort.clone(),
                request_msg.request.candidate.clone(),
            );

            let publisher_ref = Arc::clone(&publisher);
            let key = request_msg.request.message_key;
            let xid = msg.xid.clone();
            let times = Arc::clone(&self.publish_times);

            // Fire and forget, errors will show up in the log,
            // while corresponding requests will timeout.
            tokio::spawn(async move {
                let a = OffsetDateTime::now_utc().unix_timestamp_nanos() as u64;
                publisher_ref.send_message(key, msg).await.unwrap();
                // todo: move into metrics tracker
                if TRACK_PUBLISH_LATENCY {
                    let published_at = OffsetDateTime::now_utc().unix_timestamp_nanos() as u64;
                    log::debug!("b - a = {}", (published_at - a) as f32 / 1_000_000_f32);
                    match times.lock() {
                        Ok(mut map) => {
                            map.insert(xid, published_at);
                        }
                        Err(e) => {
                            log::error!("Unable to insert publish time for xid: {}. {:?}", xid, e.to_string());
                        }
                    }
                }
            });
        }
    }

    /// Passes decision to agent.
    /// Removes internal state for this XID.
    async fn handle_decision(opt_decision: Option<DecisionMessage>, state: &mut MultiMap<String, WaitingClient>) {
        if let Some(message) = opt_decision {
            let xid = &message.xid;
            Self::reply_to_agent(xid, message.decision, message.decided_at, state.get_vec(&message.xid)).await;
            state.remove(xid);
        }
    }

    /// Cleans the internal state for this XID.
    fn handle_cancellation(opt_cancellation: Option<CancelRequestChannelMessage>, state: &mut MultiMap<String, WaitingClient>) {
        if let Some(message) = opt_cancellation {
            log::warn!("The candidate '{}' is cancelled.", &message.request.candidate.xid);
            state.remove(&message.request.candidate.xid);
        }
    }

    async fn reply_to_agent(xid: &str, decision: Decision, decided_at: Option<u64>, clients: Option<&Vec<WaitingClient>>) {
        if clients.is_none() {
            log::warn!("There are no waiting clients for the candidate '{}'", xid);
            return;
        }

        let waiting_clients = clients.unwrap();

        let count = waiting_clients.len();
        let mut client_nr = 0;
        for waiting_client in waiting_clients {
            client_nr += 1;
            let response = CertificationResponse {
                xid: xid.to_string(),
                decision: decision.clone(),
                send_started_at: waiting_client.received_at,
                decided_at: decided_at.unwrap_or(0),
                decision_buffered_at: OffsetDateTime::now_utc().unix_timestamp_nanos() as u64,
                received_at: 0,
            };

            let error_message = format!(
                "Error processing XID: {}. Unable to send response {} of {} to waiting client.",
                xid, client_nr, count,
            );

            waiting_client.notify(response.clone(), error_message).await;
        }
    }
}