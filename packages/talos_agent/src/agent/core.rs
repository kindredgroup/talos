use crate::agent::decision_reader::DecisionReaderService;
use crate::agent::errors::AgentError;
use crate::agent::errors::AgentErrorKind::Certification;
use crate::agent::model::{CancelRequestChannelMessage, CertifyRequestChannelMessage};
use crate::agent::state_manager::StateManager;
use crate::api::{AgentConfig, CertificationRequest, CertificationResponse, TalosAgent};
use crate::messaging::api::{ConsumerType, DecisionMessage, PublisherType};
use crate::metrics::client::MetricsClient;
use crate::metrics::core::Metrics;
use crate::metrics::model::{EventName, MetricsReport};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::error::Elapsed;
use tokio::time::timeout;
use crate::mpsc::core::{Receiver, ReceiverWrapper, Sender, SenderWrapper};

pub struct TalosAgentImpl<TCancelTx>
where
    TCancelTx: Sender<Data=CancelRequestChannelMessage>
{
    agent_config: AgentConfig,
    tx_certify: Arc<Box<dyn Sender<Data=CertifyRequestChannelMessage>>>,
    tx_cancel: TCancelTx,
    metrics: Option<Metrics>,
    metrics_client: Arc<Option<Box<MetricsClient>>>,
}

impl <TCancelTx> TalosAgentImpl<TCancelTx>
where
    TCancelTx: Sender<Data=CancelRequestChannelMessage>
{
    pub fn new(
        agent_config: AgentConfig,
        tx_certify: Arc<Box<dyn Sender<Data=CertifyRequestChannelMessage>>>,
        tx_cancel: TCancelTx,
        metrics: Option<Metrics>,
        metrics_client: Arc<Option<Box<MetricsClient>>>,
    ) -> TalosAgentImpl<TCancelTx> {
        TalosAgentImpl {
            agent_config,
            tx_certify,
            tx_cancel,
            metrics,
            metrics_client,
        }
    }

    pub fn start<TCertifyRx, TCancelRx, TDecisionTx, TDecisionRx>(
        &self,
        rx_certify: TCertifyRx,
        rx_cancel: TCancelRx,
        tx_decision: TDecisionTx,
        rx_decision: TDecisionRx,
        publisher: Arc<Box<PublisherType>>,
        consumer: Arc<Box<ConsumerType>>,
    ) -> Result<(), AgentError>
    where
        TCertifyRx: crate::mpsc::core::Receiver<Data=CertifyRequestChannelMessage> + 'static,
        TCancelRx: crate::mpsc::core::Receiver<Data=CancelRequestChannelMessage> + 'static,
        TDecisionTx: Sender<Data=DecisionMessage> + 'static,
        TDecisionRx: crate::mpsc::core::Receiver<Data=DecisionMessage> + 'static
    {
        let agent_config = self.agent_config.clone();
        log::info!("Publisher and Consumer are ready.");

        let mc = Arc::clone(&self.metrics_client);
        // Start StateManager
        tokio::spawn(async move {
            StateManager::new(agent_config, mc).run(rx_certify, rx_cancel, rx_decision, publisher).await;
        });

        self.start_reading_decisions(tx_decision, consumer);

        Ok(())
    }

    /// Spawn the task which hosts DecisionReaderService.
    fn start_reading_decisions<TDecisionTx>(&self, tx_decision: TDecisionTx, consumer: Arc<Box<ConsumerType>>)
    where
        TDecisionTx: crate::mpsc::core::Sender<Data=DecisionMessage> + 'static
    {
        let consumer_ref = Arc::clone(&consumer);
        tokio::spawn(async move {
            let decision_reader = DecisionReaderService::new(consumer_ref, tx_decision);
            decision_reader.run().await;
        });
    }
}

#[async_trait]
impl <TCancel> TalosAgent for TalosAgentImpl<TCancel>
where
    TCancel: Sender<Data=CancelRequestChannelMessage>
{
    async fn certify(&self, request: CertificationRequest) -> Result<CertificationResponse, AgentError> {
        let (tx_ch, rx_ch) = mpsc::channel::<CertificationResponse>(1);
        let tx = SenderWrapper { tx: tx_ch };
        let mut rx = ReceiverWrapper { rx: rx_ch };

        if let Some(mc) = self.metrics_client.as_ref() {
            mc.new_event(EventName::Started, request.candidate.xid.clone());
        }

        let m = CertifyRequestChannelMessage::new(&request, Arc::new(Box::new(tx)));
        let to_state_manager = Arc::clone(&self.tx_certify);

        let max_wait: Duration = request.timeout.unwrap_or_else(|| Duration::from_millis(self.agent_config.timout_ms));

        let result: Result<Result<CertificationResponse, AgentError>, Elapsed> = timeout(max_wait, async {
            match to_state_manager.send(m).await {
                Ok(()) => match rx.recv().await {
                    Some(response) => {
                        if let Some(mc) = self.metrics_client.as_ref() {
                            mc.new_event(EventName::Finished, request.candidate.xid.clone());
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

    fn collect_metrics(&self) -> Option<MetricsReport> {
        self.metrics.as_ref().map(|m| m.collect())
    }
}
