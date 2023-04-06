use crate::agent::model::{CancelRequestChannelMessage, CertifyRequestChannelMessage};
use crate::api::{AgentConfig, CertificationResponse};
use crate::messaging::api::{CandidateMessage, Decision, DecisionMessage, PublisherType};
use crate::metrics::client::MetricsClient;
use crate::metrics::model::{EventName, Signal};
use crate::mpsc::core::{Receiver, Sender};
use multimap::MultiMap;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::task::JoinHandle;

/// Structure represents client who sent the certification request.
struct WaitingClient {
    /// Time when certification request received.
    received_at: u64,
    tx_sender: Arc<Box<dyn Sender<Data = CertificationResponse>>>,
}

impl WaitingClient {
    pub fn new(tx_sender: Arc<Box<dyn Sender<Data = CertificationResponse>>>) -> Self {
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

pub struct StateManager<TSignalTx: Sender<Data = Signal>> {
    agent_config: AgentConfig,
    metrics_client: Arc<Option<Box<MetricsClient<TSignalTx>>>>,
}

impl<TSignalTx: Sender<Data = Signal> + 'static> StateManager<TSignalTx> {
    pub fn new(agent_config: AgentConfig, metrics_client: Arc<Option<Box<MetricsClient<TSignalTx>>>>) -> StateManager<TSignalTx> {
        StateManager { agent_config, metrics_client }
    }

    // $coverage:ignore-start
    // Ignored from coverage because of infinite loop, which just delegates calls to handlers. Handlers are covered separately.
    pub async fn run<TCertifyRx, TCancelRx, TDecisionRx>(
        &self,
        mut rx_certify: TCertifyRx,
        mut rx_cancel: TCancelRx,
        mut rx_decision: TDecisionRx,
        publisher: Arc<Box<PublisherType>>,
    ) where
        TCertifyRx: Receiver<Data = CertifyRequestChannelMessage> + 'static,
        TCancelRx: Receiver<Data = CancelRequestChannelMessage> + 'static,
        TDecisionRx: Receiver<Data = DecisionMessage>,
    {
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
    // $coverage:ignore-end

    /// Passes candidate to kafka publisher and records it in the internal state.
    /// The publishing action is done asynchronously.
    async fn handle_candidate(
        &self,
        opt_candidate: Option<CertifyRequestChannelMessage>,
        publisher: Arc<Box<PublisherType>>,
        state: &mut MultiMap<String, WaitingClient>,
    ) -> Option<JoinHandle<()>> {
        if let Some(request_msg) = opt_candidate {
            let wc = WaitingClient::new(Arc::clone(&request_msg.tx_answer));
            state.insert(request_msg.request.candidate.xid.clone(), wc);

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
            Some(tokio::spawn(async move {
                publisher_ref.send_message(key, msg).await.unwrap();
                if let Some(mc) = metrics.as_ref() {
                    mc.new_event(EventName::CandidatePublished, xid.clone()).await.unwrap();
                }
            }))
        } else {
            None
        }
    }

    /// Passes decision to agent.
    /// Removes internal state for this XID.
    async fn handle_decision(
        opt_decision: Option<DecisionMessage>,
        state: &mut MultiMap<String, WaitingClient>,
        metrics_client: Arc<Option<Box<MetricsClient<TSignalTx>>>>,
    ) {
        if let Some(message) = opt_decision {
            let xid = &message.xid;
            Self::reply_to_agent(
                xid,
                message.decision,
                message.decided_at,
                message.version,
                state.get_vec(&message.xid),
                metrics_client,
            )
            .await;
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
        version: u64,
        clients: Option<&Vec<WaitingClient>>,
        metrics_client: Arc<Option<Box<MetricsClient<TSignalTx>>>>,
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
                version,
            };

            let error_message = format!(
                "Error processing XID: {}. Unable to send response {} of {} to waiting client.",
                xid, client_nr, count,
            );

            waiting_client.notify(response.clone(), error_message).await;

            if let Some(mc) = metrics_client.as_ref() {
                mc.new_event_at(EventName::Decided, xid.to_string(), decided_at.unwrap_or(0)).await.unwrap();
                mc.new_event_at(EventName::CandidateReceived, xid.to_string(), waiting_client.received_at)
                    .await
                    .unwrap();
                mc.new_event_at(EventName::DecisionReceived, xid.to_string(), decision_received_at)
                    .await
                    .unwrap();
            }
        }
    }
}

// $coverage:ignore-start
#[cfg(test)]
mod tests_waiting_client {
    use super::*;
    use crate::messaging::api::Decision::Committed;
    use async_trait::async_trait;
    use mockall::mock;
    use tokio::sync::mpsc::error::SendError;

    mock! {
        NoopSender {}

        #[async_trait]
        impl Sender for NoopSender {
            type Data = CertificationResponse;
            pub async fn send(&self, value: CertificationResponse) -> Result<(), SendError<CertificationResponse>> {}
        }
    }

    #[tokio::test]
    async fn notify_should_forward_to_channel() {
        let mut sender = MockNoopSender::new();

        sender.expect_send().withf(move |param| param.xid == *"xid1").once().returning(move |_| Ok(()));

        let response = CertificationResponse {
            xid: String::from("xid1"),
            decision: Committed,
            version: 1,
        };

        let client = WaitingClient::new(Arc::new(Box::new(sender)));
        assert!(client.received_at > 0);
        client.notify(response, String::from("Debug message when error happens")).await;
    }

    #[tokio::test]
    async fn notify_should_not_panic_on_channel_error() {
        let mut sender = MockNoopSender::new();

        let response_sample = CertificationResponse {
            xid: String::from("xid1"),
            decision: Committed,
            version: 1,
        };

        let response = response_sample.clone();
        let response_copy = response_sample.clone();

        sender.expect_send().once().returning(move |_| Err(SendError(response.clone())));

        let client = WaitingClient::new(Arc::new(Box::new(sender)));
        assert!(client.received_at > 0);
        client.notify(response_copy, String::from("Debug message when error happens")).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{CandidateData, CertificationRequest};
    use crate::messaging::api::Decision::Committed;
    use crate::messaging::api::{PublishResponse, Publisher};
    use crate::messaging::errors::MessagingError;
    use async_trait::async_trait;
    use mockall::{mock, Sequence};
    use std::time::Duration;
    use tokio::sync::mpsc::error::SendError;

    mock! {
        NoopPublisher {}

        #[async_trait]
        impl Publisher for NoopPublisher {
            async fn send_message(&self, key: String, message: CandidateMessage) -> Result<PublishResponse, MessagingError>;
        }
    }

    mock! {
        NoopSender {}

        #[async_trait]
        impl Sender for NoopSender {
            type Data = CertificationResponse;
            pub async fn send(&self, value: CertificationResponse) -> Result<(), SendError<CertificationResponse>> {}
        }
    }

    mock! {
        NoopMetricsSender {}

        #[async_trait]
        impl Sender for NoopMetricsSender {
            type Data = Signal;
            pub async fn send(&self, value: Signal) -> Result<(), SendError<Signal>> {}
        }
    }

    fn make_config() -> AgentConfig {
        AgentConfig {
            agent: String::from("agent-1"),
            cohort: String::from("cohort-1"),
            buffer_size: 10_000,
            timout_ms: 1,
        }
    }

    fn make_candidate_request(xid: String, tx_answer: MockNoopSender) -> CertifyRequestChannelMessage {
        CertifyRequestChannelMessage::new(
            &CertificationRequest {
                message_key: String::from("some key"),
                timeout: Some(Duration::from_secs(1)),
                candidate: CandidateData {
                    xid,
                    readset: vec![String::from("v1")],
                    readvers: vec![1_u64],
                    snapshot: 1_u64,
                    writeset: Vec::<String>::new(),
                },
            },
            Arc::new(Box::new(tx_answer)),
        )
    }

    fn ensure_publisher_is_invoked(cfg_copy: AgentConfig, publisher: &mut MockNoopPublisher) {
        publisher
            .expect_send_message()
            .withf(move |param_key, param_msg| {
                param_key == "some key"
                    && param_msg.xid == *"xid1"
                    && param_msg.agent == cfg_copy.agent
                    && param_msg.cohort == cfg_copy.cohort
                    && param_msg.readset == vec![String::from("v1")]
                    && param_msg.snapshot == 1
                    && param_msg.readvers == vec![1_u64]
                    && param_msg.writeset == Vec::<String>::new()
            })
            .once()
            .returning(move |_, _| Ok(PublishResponse { partition: 0, offset: 100 }));
    }

    fn new_client(tx: MockNoopSender) -> WaitingClient {
        WaitingClient::new(Arc::new(Box::new(tx)))
    }

    fn new_client_with_time(tx: MockNoopSender, time: u64) -> WaitingClient {
        WaitingClient {
            received_at: time,
            tx_sender: Arc::new(Box::new(tx)),
        }
    }

    fn expect_event_at_time(param_event: &Signal, event_name: EventName, expected_time: u64) -> bool {
        if let Signal::Start { time, event } = param_event {
            event.event_name == event_name && *time == expected_time
        } else {
            false
        }
    }

    fn expect_event_after_time(param_event: &Signal, event_name: EventName, expected_time: u64) -> bool {
        if let Signal::Start { time, event } = param_event {
            event.event_name == event_name && *time > expected_time
        } else {
            false
        }
    }

    #[tokio::test]
    async fn handle_candidate_should_publish() {
        let cfg = make_config();

        let cfg_copy = cfg.clone();
        let metrics_client: Option<Box<MetricsClient<MockNoopMetricsSender>>> = None;

        let manager = StateManager::new(cfg, Arc::new(metrics_client));

        let mut tx_answer = MockNoopSender::new();
        tx_answer.expect_send().never();

        let mut publisher = MockNoopPublisher::new();
        ensure_publisher_is_invoked(cfg_copy, &mut publisher);

        let mut state = MultiMap::<String, WaitingClient>::new();

        let xid = "xid1";
        let result = manager
            .handle_candidate(
                Some(make_candidate_request(xid.to_string(), tx_answer).clone()),
                Arc::new(Box::new(publisher)),
                &mut state,
            )
            .await;

        assert!(result.is_some());
        let handle = result.unwrap().await;
        assert!(handle.is_ok());
        assert!(state.get(xid).is_some());
    }

    #[tokio::test]
    async fn handle_candidate_should_emit_metrics() {
        let cfg = make_config();
        let cfg_copy = cfg.clone();

        let mut tx_metrics = MockNoopMetricsSender::new();
        tx_metrics
            .expect_send()
            .withf(move |param_event| expect_event_after_time(param_event, EventName::CandidatePublished, 0))
            .once()
            .returning(move |_| Ok(()));

        let metrics_client = Some(Box::new(MetricsClient { tx_destination: tx_metrics }));

        let manager = StateManager::new(cfg, Arc::new(metrics_client));

        let mut tx_answer = MockNoopSender::new();
        tx_answer.expect_send().never();

        let mut publisher = MockNoopPublisher::new();
        ensure_publisher_is_invoked(cfg_copy, &mut publisher);

        let mut state = MultiMap::<String, WaitingClient>::new();

        let xid = "xid1";
        let result = manager
            .handle_candidate(
                Some(make_candidate_request(xid.to_string(), tx_answer)),
                Arc::new(Box::new(publisher)),
                &mut state,
            )
            .await;

        assert!(result.is_some());
        let handle = result.unwrap().await;
        assert!(handle.is_ok());
        assert!(state.get(xid).is_some());
    }

    #[tokio::test]
    async fn handle_candidate_should_not_publish() {
        // No publishing to kafka if there is no request received
        let metrics_client: Option<Box<MetricsClient<MockNoopMetricsSender>>> = None;

        let manager = StateManager::new(make_config(), Arc::new(metrics_client));

        let mut tx_answer = MockNoopSender::new();
        tx_answer.expect_send().never();

        let mut publisher = MockNoopPublisher::new();
        publisher.expect_send_message().never();

        let mut state = MultiMap::<String, WaitingClient>::new();
        let result = manager.handle_candidate(None, Arc::new(Box::new(publisher)), &mut state).await;
        assert!(result.is_none());
        assert_eq!(state.len(), 0);
    }

    #[tokio::test]
    async fn handle_decision_should_notify_clients() {
        let metrics_client: Option<Box<MetricsClient<MockNoopMetricsSender>>> = None;
        let cfg = make_config();

        let xid1 = "xid1";
        let xid2 = "xid2";

        // Client 1 and 3 are waiting for the answer of our transaction and should be notified,
        // while client 2 is waiting for another transaction, hence should not be notified.

        let mut tx_answer_for_client1 = MockNoopSender::new();
        let mut tx_answer_for_client2 = MockNoopSender::new();
        let mut tx_answer_for_client3 = MockNoopSender::new();

        tx_answer_for_client2.expect_send().never();
        tx_answer_for_client1
            .expect_send()
            .withf(move |param| param.xid == *"xid1")
            .once()
            .returning(move |_| Ok(()));
        tx_answer_for_client3
            .expect_send()
            .withf(move |param| param.xid == *"xid1")
            .once()
            .returning(move |_| Ok(()));

        let mut state = MultiMap::<String, WaitingClient>::new();
        let client1 = new_client(tx_answer_for_client1);
        let client2 = new_client(tx_answer_for_client2);
        let client3 = new_client(tx_answer_for_client3);

        state.insert(xid1.to_string(), client1);
        state.insert(xid2.to_string(), client2);
        state.insert(xid1.to_string(), client3);

        let decision = DecisionMessage {
            xid: xid1.to_string(),
            agent: cfg.agent.to_string(),
            cohort: cfg.cohort.to_string(),
            decision: Committed,
            suffix_start: 2,
            version: 2,
            safepoint: None,
            decided_at: Some(999),
        };

        assert_eq!(state.len(), 2);
        StateManager::handle_decision(Some(decision), &mut state, Arc::new(metrics_client)).await;
        assert_eq!(state.len(), 1);
        assert!(state.get(xid1).is_none());
        assert!(state.get(xid2).is_some());
    }

    #[tokio::test]
    async fn handle_decision_should_not_panic_if_clients_are_waiting() {
        let metrics_client: Option<Box<MetricsClient<MockNoopMetricsSender>>> = None;
        let cfg = make_config();

        let mut tx_for_client = MockNoopSender::new();
        tx_for_client.expect_send().never();

        let mut state = MultiMap::<String, WaitingClient>::new();
        let decision = DecisionMessage {
            xid: "xid1".to_string(),
            agent: cfg.agent.to_string(),
            cohort: cfg.cohort.to_string(),
            decision: Committed,
            suffix_start: 2,
            version: 2,
            safepoint: None,
            decided_at: Some(999),
        };

        assert!(state.is_empty());
        StateManager::handle_decision(Some(decision), &mut state, Arc::new(metrics_client)).await;
        assert!(state.is_empty());
    }

    #[tokio::test]
    async fn handle_decision_should_emit_metrics() {
        // time when event was decided (sent by Talos)
        let candidate_time_at = 888;
        let decided_at = 999;

        let mut seq = Sequence::new();
        let mut tx_metrics = MockNoopMetricsSender::new();
        tx_metrics
            .expect_send()
            .withf(move |param_event| expect_event_at_time(param_event, EventName::Decided, decided_at))
            .once()
            .in_sequence(&mut seq)
            .returning(move |_| Ok(()));

        tx_metrics
            .expect_send()
            .withf(move |param_event| expect_event_at_time(param_event, EventName::CandidateReceived, candidate_time_at))
            .once()
            .in_sequence(&mut seq)
            .returning(move |_| Ok(()));

        tx_metrics
            .expect_send()
            .withf(move |param_event| expect_event_after_time(param_event, EventName::DecisionReceived, 0))
            .once()
            .in_sequence(&mut seq)
            .returning(move |_| Ok(()));

        let metrics_client = Some(Box::new(MetricsClient { tx_destination: tx_metrics }));

        let mut tx_client = MockNoopSender::new();
        tx_client
            .expect_send()
            .withf(move |param| param.xid == *"xid1")
            .once()
            .returning(move |_| Ok(()));

        let mut state = MultiMap::<String, WaitingClient>::new();
        state.insert("xid1".to_string(), new_client_with_time(tx_client, candidate_time_at));

        let cfg = make_config();
        let decision = DecisionMessage {
            xid: "xid1".to_string(),
            agent: cfg.agent.to_string(),
            cohort: cfg.cohort.to_string(),
            decision: Committed,
            suffix_start: 2,
            version: 2,
            safepoint: None,
            decided_at: Some(decided_at),
        };

        assert_eq!(state.len(), 1);
        StateManager::handle_decision(Some(decision), &mut state, Arc::new(metrics_client)).await;
        assert!(state.is_empty());
    }

    #[tokio::test]
    async fn handle_cancellation_should_clean_state() {
        let mut state = MultiMap::new();
        state.insert("xid1".to_string(), new_client(MockNoopSender::new()));
        state.insert("xid2".to_string(), new_client(MockNoopSender::new()));

        let cancel_req = CancelRequestChannelMessage {
            request: CertificationRequest {
                message_key: "some-key".to_string(),
                timeout: Some(Duration::from_secs(1)),
                candidate: CandidateData {
                    xid: "xid1".to_string(),
                    readset: Vec::<String>::new(),
                    readvers: Vec::<u64>::new(),
                    snapshot: 0,
                    writeset: Vec::<String>::new(),
                },
            },
        };

        let copy = cancel_req.clone();
        format!("debug and clone coverage: {:?}", copy);

        StateManager::<MockNoopMetricsSender>::handle_cancellation(Some(cancel_req), &mut state);

        assert!(state.get("xid1").is_none());
        assert!(state.get("xid2").is_some());
        assert_eq!(state.get_vec("xid2").unwrap().len(), 1);
    }
}
// $coverage:ignore-end
