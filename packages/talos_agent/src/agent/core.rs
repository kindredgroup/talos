use crate::agent::decision_reader::DecisionReaderService;
use crate::agent::errors::AgentError;
use crate::agent::errors::AgentErrorKind::Certification;
use crate::agent::model::{CancelRequestChannelMessage, CertifyRequestChannelMessage};
use crate::agent::state_manager::StateManager;
use crate::api::{AgentConfig, CertificationRequest, CertificationResponse, TalosAgent};
use crate::messaging::api::{ConsumerType, PublisherType, TraceableDecision};
use crate::metrics::client::MetricsClient;
use crate::metrics::core::Metrics;
use crate::metrics::model::{EventName, MetricsReport, Signal};
use crate::mpsc::core::{Receiver, Sender};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::error::Elapsed;
use tokio::time::timeout;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub struct TalosAgentImpl<TCancelTx, TSignalTx, TRespChFactory, TResponseTx, TResponseRx>
where
    TCancelTx: Sender<Data = CancelRequestChannelMessage>,
    TSignalTx: Sender<Data = Signal>,
    TResponseTx: Sender<Data = CertificationResponse> + 'static,
    TResponseRx: Receiver<Data = CertificationResponse> + 'static,
    TRespChFactory: Fn() -> (TResponseTx, TResponseRx) + Send,
{
    agent_config: AgentConfig,
    tx_certify: Arc<Box<dyn Sender<Data = CertifyRequestChannelMessage>>>,
    tx_cancel: TCancelTx,
    metrics: Option<Metrics>,
    metrics_client: Arc<Option<Box<MetricsClient<TSignalTx>>>>,
    channel_factory: TRespChFactory,
}

impl<TCancelTx, TSignalTx, TRespChFactory, TResponseTx, TResponseRx> TalosAgentImpl<TCancelTx, TSignalTx, TRespChFactory, TResponseTx, TResponseRx>
where
    TCancelTx: Sender<Data = CancelRequestChannelMessage>,
    TSignalTx: Sender<Data = Signal> + 'static,
    TResponseTx: Sender<Data = CertificationResponse> + 'static,
    TResponseRx: Receiver<Data = CertificationResponse> + 'static,
    TRespChFactory: Fn() -> (TResponseTx, TResponseRx) + Send + Sync,
{
    pub fn new(
        agent_config: AgentConfig,
        tx_certify: Arc<Box<dyn Sender<Data = CertifyRequestChannelMessage>>>,
        tx_cancel: TCancelTx,
        metrics: Option<Metrics>,
        metrics_client: Arc<Option<Box<MetricsClient<TSignalTx>>>>,
        channel_factory: TRespChFactory,
    ) -> TalosAgentImpl<TCancelTx, TSignalTx, TRespChFactory, TResponseTx, TResponseRx> {
        TalosAgentImpl {
            agent_config,
            tx_certify,
            tx_cancel,
            metrics,
            metrics_client,
            channel_factory,
        }
    }

    // $coverage:ignore-start
    // There are no tests for thread spawning
    pub fn start<TCertifyRx, TCancelRx, TDecisionTx, TDecisionRx>(
        &self,
        rx_certify: TCertifyRx,
        rx_cancel: TCancelRx,
        tx_decision: TDecisionTx,
        rx_decision: TDecisionRx,
        publisher: Arc<Box<PublisherType>>,
        consumer: Arc<Box<ConsumerType>>,
    ) -> AgentServices
    where
        TCertifyRx: Receiver<Data = CertifyRequestChannelMessage> + 'static,
        TCancelRx: Receiver<Data = CancelRequestChannelMessage> + 'static,
        TDecisionTx: Sender<Data = TraceableDecision> + 'static,
        TDecisionRx: Receiver<Data = TraceableDecision> + 'static,
    {
        let agent_config = self.agent_config.clone();
        tracing::info!("Publisher and Consumer are ready.");

        let metrics_client = Arc::clone(&self.metrics_client);
        let handle_to_manager_task = tokio::spawn(async move {
            StateManager::new(agent_config, metrics_client)
                .run(rx_certify, rx_cancel, rx_decision, publisher)
                .await;
        });

        let consumer_ref = Arc::clone(&consumer);
        let handle_to_decision_reader_task = tokio::spawn(async move {
            DecisionReaderService::new(consumer_ref, tx_decision).run().await;
        });

        AgentServices {
            state_manager: handle_to_manager_task,
            decision_reader: handle_to_decision_reader_task,
        }
    }
    // $coverage:ignore-end
}

pub struct AgentServices {
    pub state_manager: JoinHandle<()>,
    pub decision_reader: JoinHandle<()>,
}

#[async_trait]
impl<TCancel, TSignalTx, TRespChFactory, TResponseTx, TResponseRx> TalosAgent for TalosAgentImpl<TCancel, TSignalTx, TRespChFactory, TResponseTx, TResponseRx>
where
    TCancel: Sender<Data = CancelRequestChannelMessage>,
    TSignalTx: Sender<Data = Signal> + 'static,
    TResponseTx: Sender<Data = CertificationResponse> + 'static,
    TResponseRx: Receiver<Data = CertificationResponse> + 'static,
    TRespChFactory: Fn() -> (TResponseTx, TResponseRx) + Send + Sync,
{
    #[tracing::instrument(
        name = "agent_certify",
        skip(self, request),
        fields(xid = %request.candidate.xid)
    )]
    async fn certify(&self, request: CertificationRequest) -> Result<CertificationResponse, AgentError> {
        let (tx, mut rx) = (self.channel_factory)();
        if let Some(mc) = self.metrics_client.as_ref() {
            mc.new_event(EventName::Started, request.candidate.xid.clone()).await.unwrap();
        }

        let m = CertifyRequestChannelMessage::new(
            &request,
            Arc::new(Box::new(tx)),
            tracing::Span::current().id().map(|id| (tracing::Span::current().context(), id)),
        );
        let to_state_manager = Arc::clone(&self.tx_certify);

        let max_wait: Duration = request.timeout.unwrap_or_else(|| Duration::from_millis(self.agent_config.timeout_ms as u64));

        let result: Result<Result<CertificationResponse, AgentError>, Elapsed> = timeout(max_wait, async {
            match to_state_manager.send(m).await {
                Ok(()) => match rx.recv().await {
                    Some(response) => {
                        if let Some(mc) = self.metrics_client.as_ref() {
                            mc.new_event(EventName::Finished, request.candidate.xid.clone()).await.unwrap();
                        }

                        Ok(response)
                    }
                    None => Err(AgentError {
                        kind: Certification {
                            xid: request.candidate.xid.clone(),
                        },
                        reason: "No response from state manager".to_string(),
                        cause: None,
                    }),
                },
                Err(e) => Err(e.into()),
            }
        })
        .await;

        let xid = request.candidate.xid.clone();
        match result {
            Ok(rslt_certify) => rslt_certify,
            Err(_) => {
                let _ = self.tx_cancel.send(CancelRequestChannelMessage { request }).await;
                Err(AgentError::new_certify_timout(xid, max_wait.as_millis()))
            }
        }
    }

    async fn collect_metrics(&self) -> Option<MetricsReport> {
        match self.metrics.as_ref() {
            Some(m) => m.collect().await,
            None => None,
        }
    }
}

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::CandidateData;
    use crate::messaging::api::Decision::Committed;
    use async_trait::async_trait;
    use mockall::{mock, Sequence};
    use tokio::sync::mpsc::error::SendError;

    // mocking certify sender
    mock! {
        NoopCertifyTx {}

        #[async_trait]
        impl Sender for NoopCertifyTx {
            type Data = CertifyRequestChannelMessage;
            pub async fn send(&self, value: CertifyRequestChannelMessage) -> Result<(), SendError<CertifyRequestChannelMessage>> {}
        }
    }

    // mocking cancel sender
    mock! {
        NoopCancelTx {}

        #[async_trait]
        impl Sender for NoopCancelTx {
            type Data = CancelRequestChannelMessage;
            pub async fn send(&self, value: CancelRequestChannelMessage) -> Result<(), SendError<CancelRequestChannelMessage>> {}
        }
    }

    // mocking metrics sender
    mock! {
        NoopMetricsTx {}

        #[async_trait]
        impl Sender for NoopMetricsTx {
            type Data = Signal;
            pub async fn send(&self, value: Signal) -> Result<(), SendError<Signal>> {}
        }
    }

    // cert response sender
    mock! {
        NoopCertResponseTx {}

        #[async_trait]
        impl Sender for NoopCertResponseTx {
            type Data = CertificationResponse;
            pub async fn send(&self, value: CertificationResponse) -> Result<(), SendError<CertificationResponse>> {}
        }
    }

    // cert response receiver
    mock! {
        NoopCertResponseRx {}

        #[async_trait]
        impl Receiver for NoopCertResponseRx {
            type Data = CertificationResponse;
            pub async fn recv(&mut self) -> Option<CertificationResponse>;
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

    fn sample_candidate() -> CandidateData {
        CandidateData {
            xid: "xid1".to_string(),
            readset: Vec::<String>::new(),
            readvers: Vec::<u64>::new(),
            snapshot: 1_u64,
            writeset: Vec::<String>::new(),
            statemap: None,
            on_commit: None,
        }
    }

    fn sample_request(sample_candidate: CandidateData) -> CertificationRequest {
        CertificationRequest {
            message_key: "k1".to_string(),
            candidate: sample_candidate,
            timeout: Some(Duration::from_secs(1)),
            headers: None,
            certification_started_at: 0,
            request_created_at: 0,
        }
    }

    fn setup_certify_with_ok_response(tx_certify: &mut MockNoopCertifyTx, tx_cancel: &mut MockNoopCancelTx) {
        tx_certify
            .expect_send()
            .withf(move |param| param.request.candidate.xid == *"xid1")
            .once()
            .returning(move |_| Ok(()));

        tx_cancel.expect_send().never();
    }

    fn mock_response_channel() -> (MockNoopCertResponseTx, MockNoopCertResponseRx) {
        let mut tx_response = MockNoopCertResponseTx::new();
        let mut rx_response = MockNoopCertResponseRx::new();

        tx_response.expect_send().returning(move |_| Ok(()));

        rx_response.expect_recv().returning(move || {
            Some(CertificationResponse {
                xid: "xid1".to_string(),
                decision: Committed,
                version: 1,
                safepoint: None,
                conflict: None,
            })
        });
        (tx_response, rx_response)
    }

    fn expect_event(value: &Signal, event_name: EventName) -> bool {
        if let Signal::Start { time, event } = value {
            *time > 0 && event.id == *"xid1" && event.event_name == event_name
        } else {
            false
        }
    }

    fn assert_expected_certify_error(result: Result<CertificationResponse, AgentError>) {
        assert!(result.is_err());
        let is_error_returned = match result {
            Err(error) => {
                if let Certification { xid } = error.kind {
                    xid == *"xid1"
                } else {
                    false
                }
            }

            _ => false,
        };

        assert!(is_error_returned);
    }

    #[tokio::test]
    async fn certify_without_metrics() {
        let sample_candidate = sample_candidate();

        let cfg = make_config();
        let mut tx_certify = MockNoopCertifyTx::new();
        let mut tx_cancel = MockNoopCancelTx::new();
        let metrics: Option<Metrics> = None;
        let metrics_client: Option<Box<MetricsClient<MockNoopMetricsTx>>> = None;

        // record internal interactions
        setup_certify_with_ok_response(&mut tx_certify, &mut tx_cancel);

        let agent_impl = TalosAgentImpl::new(
            cfg,
            Arc::new(Box::new(tx_certify)),
            tx_cancel,
            metrics,
            Arc::new(metrics_client),
            mock_response_channel,
        );

        let agent: Box<dyn TalosAgent> = Box::new(agent_impl);
        let request = sample_request(sample_candidate);
        let result = agent.certify(request).await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.xid, "xid1".to_string());
        assert!(agent.collect_metrics().await.is_none());
    }

    #[tokio::test]
    async fn certify_should_emit_metrics() {
        let sample_candidate = sample_candidate();
        let cfg = make_config();

        // record internal interactions

        let mut seq = Sequence::new();
        let mut tx_metrics = MockNoopMetricsTx::new();
        tx_metrics
            .expect_send()
            .withf(|param| expect_event(param, EventName::Started))
            .once()
            .in_sequence(&mut seq)
            .returning(|_| Ok(()));
        let metrics = Metrics::new();

        let mut tx_certify = MockNoopCertifyTx::new();
        let mut tx_cancel = MockNoopCancelTx::new();

        setup_certify_with_ok_response(&mut tx_certify, &mut tx_cancel);

        tx_metrics
            .expect_send()
            .withf(|param| expect_event(param, EventName::Finished))
            .once()
            .in_sequence(&mut seq)
            .returning(|_| Ok(()));

        let metrics_client = MetricsClient { tx_destination: tx_metrics };

        let agent_impl = TalosAgentImpl::new(
            cfg,
            Arc::new(Box::new(tx_certify)),
            tx_cancel,
            Some(metrics),
            Arc::new(Some(Box::new(metrics_client))),
            mock_response_channel,
        );

        let agent: Box<dyn TalosAgent> = Box::new(agent_impl);
        let request = sample_request(sample_candidate);
        let result = agent.certify(request).await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.xid, "xid1".to_string());
        assert!(agent.collect_metrics().await.is_none());
    }

    #[tokio::test]
    async fn certify_should_return_agent_error_on_send() {
        let sample_candidate = sample_candidate();

        let cfg = make_config();
        let mut tx_certify = MockNoopCertifyTx::new();
        let mut tx_cancel = MockNoopCancelTx::new();
        let metrics: Option<Metrics> = None;
        let metrics_client: Option<Box<MetricsClient<MockNoopMetricsTx>>> = None;

        // record internal interactions
        tx_certify
            .expect_send()
            .withf(move |param| param.request.candidate.xid == *"xid1")
            .once()
            .returning(move |data| Err(SendError(data)));

        tx_cancel.expect_send().never();

        let agent_impl = TalosAgentImpl::new(cfg, Arc::new(Box::new(tx_certify)), tx_cancel, metrics, Arc::new(metrics_client), || {
            let mut tx_response = MockNoopCertResponseTx::new();
            let mut rx_response = MockNoopCertResponseRx::new();
            tx_response.expect_send().never();
            rx_response.expect_recv().never();
            (tx_response, rx_response)
        });

        let agent: Box<dyn TalosAgent> = Box::new(agent_impl);
        let request = sample_request(sample_candidate);
        let result = agent.certify(request).await;
        assert_expected_certify_error(result);
        assert!(agent.collect_metrics().await.is_none());
    }

    #[tokio::test]
    async fn certify_should_return_agent_error_on_receive() {
        let sample_candidate = sample_candidate();

        let cfg = make_config();
        let mut tx_certify = MockNoopCertifyTx::new();
        let mut tx_cancel = MockNoopCancelTx::new();
        let metrics: Option<Metrics> = None;
        let metrics_client: Option<Box<MetricsClient<MockNoopMetricsTx>>> = None;

        // record internal interactions
        setup_certify_with_ok_response(&mut tx_certify, &mut tx_cancel);

        let agent_impl = TalosAgentImpl::new(cfg, Arc::new(Box::new(tx_certify)), tx_cancel, metrics, Arc::new(metrics_client), || {
            let mut tx_response = MockNoopCertResponseTx::new();
            let mut rx_response = MockNoopCertResponseRx::new();
            tx_response.expect_send().never();
            rx_response.expect_recv().returning(move || None);
            (tx_response, rx_response)
        });

        let agent: Box<dyn TalosAgent> = Box::new(agent_impl);
        let request = sample_request(sample_candidate);
        let result = agent.certify(request).await;
        assert_expected_certify_error(result);
        assert!(agent.collect_metrics().await.is_none());
    }
}
// $coverage:ignore-end
