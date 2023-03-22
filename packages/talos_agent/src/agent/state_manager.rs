use crate::agent::model::{CancelRequestChannelMessage, CertifyRequestChannelMessage};
use crate::api::{AgentConfig, CertificationResponse};
use crate::messaging::api::{CandidateMessage, Decision, DecisionMessage, PublisherType};
use crate::metrics::client::MetricsClient;
use crate::metrics::model::EventName;
use multimap::MultiMap;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::sync::mpsc::{Receiver, Sender};

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
    metrics_client: Arc<Option<Box<MetricsClient>>>,
}

impl StateManager {
    pub fn new(agent_config: AgentConfig, metrics_client: Arc<Option<Box<MetricsClient>>>) -> StateManager {
        StateManager { agent_config, metrics_client }
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
            let mc = Arc::clone(&self.metrics_client);
            tokio::select! {
                rslt_request_msg = rx_certify.recv() => {
                    self.handle_candidate(rslt_request_msg, publisher.clone(), &mut state).await;
                }

                rslt_cancel_request_msg = rx_cancel.recv() => {
                    Self::handle_cancellation(rslt_cancel_request_msg, &mut state);
                }

                rslt_decision_msg = rx_decision.recv() => {
                    Self::handle_decision(rslt_decision_msg, &mut state, mc).await;
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
            let metrics = Arc::clone(&self.metrics_client);

            // Fire and forget, errors will show up in the log,
            // while corresponding requests will timeout.
            tokio::spawn(async move {
                publisher_ref.send_message(key, msg).await.unwrap();
                if let Some(mc) = metrics.as_ref() {
                    mc.new_event(EventName::CandidatePublished, xid.clone());
                }
            });
        }
    }

    /// Passes decision to agent.
    /// Removes internal state for this XID.
    async fn handle_decision(
        opt_decision: Option<DecisionMessage>,
        state: &mut MultiMap<String, WaitingClient>,
        metrics_client: Arc<Option<Box<MetricsClient>>>,
    ) {
        if let Some(message) = opt_decision {
            let xid = &message.xid;
            Self::reply_to_agent(xid, message.decision, message.decided_at, state.get_vec(&message.xid), metrics_client).await;
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

    async fn reply_to_agent(
        xid: &str,
        decision: Decision,
        decided_at: Option<u64>,
        clients: Option<&Vec<WaitingClient>>,
        metrics_client: Arc<Option<Box<MetricsClient>>>,
    ) {
        if clients.is_none() {
            log::warn!("There are no waiting clients for the candidate '{}'", xid);
            return;
        }

        let waiting_clients = clients.unwrap();
        let decision_received_at = OffsetDateTime::now_utc().unix_timestamp_nanos() as u64;
        let count = waiting_clients.len();
        let mut client_nr = 0;
        for waiting_client in waiting_clients {
            client_nr += 1;
            let response = CertificationResponse {
                xid: xid.to_string(),
                decision: decision.clone(),
            };

            let error_message = format!(
                "Error processing XID: {}. Unable to send response {} of {} to waiting client.",
                xid, client_nr, count,
            );

            waiting_client.notify(response.clone(), error_message).await;

            if let Some(mc) = metrics_client.as_ref() {
                mc.new_event_at(EventName::Decided, xid.to_string(), decided_at.unwrap_or(0));
                mc.new_event_at(EventName::CandidateReceived, xid.to_string(), waiting_client.received_at);
                mc.new_event_at(EventName::DecisionReceived, xid.to_string(), decision_received_at);
            }
        }
    }
}
