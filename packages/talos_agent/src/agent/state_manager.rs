use crate::agent::model::{CancelRequestChannelMessage, CertifyRequestChannelMessage};
use crate::api::{AgentConfig, CertificationResponse};
use crate::messaging::api::{CandidateMessage, DecisionMessage, PublisherType, TraceableDecision};
use crate::metrics::client::MetricsClient;
use crate::metrics::model::{EventName, Signal};
use crate::mpsc::core::{Receiver, Sender};
use multimap::MultiMap;
use opentelemetry::global;
use std::sync::Arc;
use talos_common_utils::otel::propagated_context::PropagatedSpanContextData;
use time::OffsetDateTime;
use tokio::task::JoinHandle;

use tracing::Instrument;

use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Structure represents client who sent the certification request.
pub struct WaitingClient {
    /// Time when certification request received.
    pub received_at: u64,
    tx_sender: Arc<Box<dyn Sender<Data = CertificationResponse>>>,
}

impl WaitingClient {
    pub fn new(tx_sender: Arc<Box<dyn Sender<Data = CertificationResponse>>>) -> Self {
        WaitingClient {
            received_at: OffsetDateTime::now_utc().unix_timestamp_nanos() as u64,
            tx_sender,
        }
    }
    pub async fn notify(&self, response: CertificationResponse, error_message: String) {
        if let Err(e) = self.tx_sender.send(response).await {
            tracing::error!("{}. Error: {}", error_message, e);
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
        TDecisionRx: Receiver<Data = TraceableDecision>,
    {
        let mut state: MultiMap<String, WaitingClient> = MultiMap::new();

        loop {
            let mc = Arc::clone(&self.metrics_client);
            let publisher_ref = Arc::clone(&publisher);
            tokio::select! {
                rslt_request_msg = rx_certify.recv() => {
                    if let Some(request_msg) = rslt_request_msg {
                        let span = tracing::span!(parent: request_msg.parent_span.clone().map(|(_, id)| id), tracing::Level::INFO, "agent_core_handle_candidate");
                        self.handle_candidate(request_msg, publisher_ref, &mut state).instrument(span).await;
                    }
                }

                rslt_cancel_request_msg = rx_cancel.recv() => {
                    Self::handle_cancellation(rslt_cancel_request_msg, &mut state);
                }

                rslt_decision_msg = rx_decision.recv() => {
                    if rslt_decision_msg.is_none() {
                        continue;
                    }

                    if let Some(decision_msg) = rslt_decision_msg {
                        let propagated_context = PropagatedSpanContextData::new_with_data(decision_msg.raw_span_context);
                        let span_context = global::get_text_map_propagator(|propagator| {
                            propagator.extract(&propagated_context)
                        });
                        let span = tracing::info_span!("agent_core_handle_decision");
                        span.set_parent(span_context);
                        let fut_handler = Self::handle_decision(Some(decision_msg.decision), &mut state, mc);
                        fut_handler.instrument(span).await;
                    }
                }
            }
        }
    }
    // $coverage:ignore-end

    async fn handle_candidate(
        &self,
        request_msg: CertifyRequestChannelMessage,
        publisher: Arc<Box<PublisherType>>,
        state: &mut MultiMap<String, WaitingClient>,
    ) -> Option<JoinHandle<()>> {
        let wc = WaitingClient::new(Arc::clone(&request_msg.tx_answer));
        let key = request_msg.request.message_key;
        let xid = request_msg.request.candidate.xid.clone();
        let metrics = Arc::clone(&self.metrics_client);

        state.insert(xid.clone(), wc);
        if let Some(mc) = metrics.as_ref() {
            mc.new_event(EventName::CandidateEnqueuedInAgent, xid.clone()).await.unwrap();
        }

        let msg = CandidateMessage::new(
            self.agent_config.agent.clone(),
            self.agent_config.cohort.clone(),
            request_msg.request.candidate.clone(),
            request_msg.request.certification_started_at,
            request_msg.request.request_created_at,
            0,
        );

        let publisher_ref = Arc::clone(&publisher);

        // Fire and forget, errors will show up in the log,
        // while corresponding requests will timeout.
        if let Some(mc) = metrics.as_ref() {
            mc.new_event(EventName::CandidatePublishTaskStarting, xid.clone()).await.unwrap();
        }

        let span = tracing::info_span!("publisher_service_send_message", xid = xid.clone());
        let publish_async = async move {
            if let Some(mc) = metrics.as_ref() {
                mc.new_event(EventName::CandidatePublishStarted, xid.clone()).await.unwrap();
            }
            let _ = publisher_ref
                .send_message(key, msg, request_msg.request.headers, request_msg.parent_span.map(|(ctx, _)| ctx))
                .await;

            if let Some(mc) = metrics.as_ref() {
                mc.new_event(EventName::CandidatePublished, xid.clone()).await.unwrap();
            }
        };

        Some(tokio::spawn(publish_async.instrument(span)))
    }

    /// Passes decision to agent.
    /// Removes internal state for this XID.
    // #[tracing::instrument(name = "agent_core_handle_decision", skip_all)]
    async fn handle_decision(
        opt_decision: Option<DecisionMessage>,
        state: &mut MultiMap<String, WaitingClient>,
        metrics_client: Arc<Option<Box<MetricsClient<TSignalTx>>>>,
    ) {
        if let Some(message) = opt_decision {
            let xid = &message.xid;
            Self::reply_to_agent(xid, message.clone(), state.get_vec(&message.xid), metrics_client).await;
            state.remove(xid);
        }
    }

    /// Cleans the internal state for this XID.
    fn handle_cancellation(opt_cancellation: Option<CancelRequestChannelMessage>, state: &mut MultiMap<String, WaitingClient>) {
        if let Some(message) = opt_cancellation {
            tracing::warn!("The candidate '{}' is cancelled.", &message.request.candidate.xid);
            state.remove(&message.request.candidate.xid);
        }
    }

    #[tracing::instrument(
        name = "agent_core_reply"
        skip_all,
        fields(
            xid,
            decision = %message.decision
        ),
    )]
    async fn reply_to_agent(
        xid: &str,
        message: DecisionMessage,
        clients: Option<&Vec<WaitingClient>>,
        metrics_client: Arc<Option<Box<MetricsClient<TSignalTx>>>>,
    ) {
        if clients.is_none() {
            tracing::warn!("There are no waiting clients for the candidate '{}'", xid);
            return;
        }

        let waiting_clients = clients.unwrap();
        let decision_received_at = OffsetDateTime::now_utc().unix_timestamp_nanos() as u64;
        let (candidate_received, decision_created_at) = if let Some(metrics) = message.metrics {
            (metrics.candidate_received as u64, metrics.decision_created_at as u64)
        } else {
            (decision_received_at, decision_received_at)
        };

        let count = waiting_clients.len();
        let mut client_nr = 0;
        for waiting_client in waiting_clients {
            client_nr += 1;
            let response = CertificationResponse {
                xid: xid.to_string(),
                decision: message.decision.clone(),
                version: message.version,
                safepoint: message.safepoint,
                conflict: match message.conflicts {
                    None => None,
                    Some(ref list) => {
                        if list.is_empty() {
                            None
                        } else {
                            Some(list[0].clone())
                        }
                    }
                },
            };

            let error_message = format!(
                "Error processing XID: {}. Unable to send response {} of {} to waiting client.",
                xid, client_nr, count,
            );

            if let Some(mc) = metrics_client.as_ref() {
                mc.new_event_at(EventName::CandidateReceived, xid.to_string(), waiting_client.received_at)
                    .await
                    .unwrap();
                mc.new_event_at(EventName::CandidateReceivedByTalos, xid.to_string(), candidate_received)
                    .await
                    .unwrap();
                mc.new_event_at(EventName::Decided, xid.to_string(), decision_created_at).await.unwrap();
                mc.new_event_at(EventName::DecisionReceived, xid.to_string(), decision_received_at)
                    .await
                    .unwrap();
            }
            waiting_client.notify(response.clone(), error_message).await;
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
            safepoint: None,
            conflict: None,
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
            safepoint: None,
            conflict: None,
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
    use crate::messaging::api::{PublishResponse, Publisher, TxProcessingTimeline};
    use crate::messaging::errors::MessagingError;
    use async_trait::async_trait;
    use mockall::{mock, Sequence};
    use std::collections::HashMap;
    use std::time::Duration;
    use tokio::sync::mpsc::error::SendError;

    mock! {
        NoopPublisher {}

        #[async_trait]
        impl Publisher for NoopPublisher {
            async fn send_message(&self, key: String, message: CandidateMessage, headers: Option<HashMap<String,String>>, parent_span_ctx: Option<opentelemetry::Context>) -> Result<PublishResponse, MessagingError>;
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
            timeout_ms: 1,
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
                    statemap: None,
                    on_commit: None,
                },
                headers: None,
                certification_started_at: 0,
                request_created_at: 0,
            },
            Arc::new(Box::new(tx_answer)),
            None,
        )
    }

    fn ensure_publisher_is_invoked(cfg_copy: AgentConfig, publisher: &mut MockNoopPublisher) {
        publisher
            .expect_send_message()
            .withf(move |param_key, param_msg, _headers, _span_ctx| {
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
            .returning(move |_, _, _, _| Ok(PublishResponse { partition: 0, offset: 100 }));
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
                make_candidate_request(xid.to_string(), tx_answer).clone(),
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
            .withf(move |param_event| expect_event_after_time(param_event, EventName::CandidateEnqueuedInAgent, 0))
            .once()
            .returning(move |_| Ok(()));

        tx_metrics
            .expect_send()
            .withf(move |param_event| expect_event_after_time(param_event, EventName::CandidatePublishTaskStarting, 0))
            .once()
            .returning(move |_| Ok(()));

        tx_metrics
            .expect_send()
            .withf(move |param_event| expect_event_after_time(param_event, EventName::CandidatePublishStarted, 0))
            .once()
            .returning(move |_| Ok(()));

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
            .handle_candidate(make_candidate_request(xid.to_string(), tx_answer), Arc::new(Box::new(publisher)), &mut state)
            .await;

        assert!(result.is_some());
        let handle = result.unwrap().await;
        assert!(handle.is_ok());
        assert!(state.get(xid).is_some());
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
            conflicts: None,
            metrics: None,
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
            conflicts: None,
            metrics: None,
        };

        assert!(state.is_empty());
        StateManager::handle_decision(Some(decision), &mut state, Arc::new(metrics_client)).await;
        assert!(state.is_empty());
    }

    #[tokio::test]
    async fn handle_decision_should_emit_metrics() {
        // env_logger::builder().format_timestamp_millis().init();

        // time when event was decided (sent by Talos)
        let candidate_time_at = 888;
        let candidate_received_at = 900;
        let decided_at = 999;

        let mut seq = Sequence::new();
        let mut tx_metrics = MockNoopMetricsSender::new();

        tx_metrics
            .expect_send()
            .withf(move |param_event| expect_event_at_time(param_event, EventName::CandidateReceived, candidate_time_at))
            .once()
            .in_sequence(&mut seq)
            .returning(move |_| Ok(()));

        tx_metrics
            .expect_send()
            .withf(move |param_event| expect_event_at_time(param_event, EventName::CandidateReceivedByTalos, candidate_received_at))
            .once()
            .in_sequence(&mut seq)
            .returning(move |_| Ok(()));

        tx_metrics
            .expect_send()
            .withf(move |param_event| expect_event_at_time(param_event, EventName::Decided, decided_at))
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
            conflicts: None,
            metrics: Some(TxProcessingTimeline {
                candidate_published: candidate_time_at as i128,
                candidate_received: candidate_received_at as i128,
                decision_created_at: decided_at as i128,
                ..TxProcessingTimeline::default()
            }),
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
                    statemap: None,
                    on_commit: None,
                },
                headers: None,
                certification_started_at: 0,
                request_created_at: 0,
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
