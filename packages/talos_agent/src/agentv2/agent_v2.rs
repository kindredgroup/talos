use crate::agentv2::model::{CancelRequestChannelMessage, CertifyRequestChannelMessage};
use crate::agentv2::state_manager::StateManager;
use crate::api::{AgentConfig, CertificationRequest, CertificationResponse, KafkaConfig, TalosAgent, TalosIntegrationType};
use async_trait::async_trait;
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};

pub struct TalosAgentImplV2 {
    rx_certify: Sender<CertifyRequestChannelMessage>,
    _rx_cancel: Sender<CancelRequestChannelMessage>,
}

impl TalosAgentImplV2 {
    pub async fn new(
        agent_config: AgentConfig,
        kafka_config: Option<KafkaConfig>,
        int_type: &TalosIntegrationType,
        certify: (Sender<CertifyRequestChannelMessage>, Receiver<CertifyRequestChannelMessage>),
        cancel: (Sender<CancelRequestChannelMessage>, Receiver<CancelRequestChannelMessage>),
    ) -> Result<TalosAgentImplV2, String> {
        let state_manager = StateManager::new(agent_config, kafka_config, int_type).unwrap();

        state_manager.start(certify.1, cancel.1).await;

        Ok(TalosAgentImplV2 {
            rx_certify: certify.0,
            _rx_cancel: cancel.0,
        })
    }
}

#[async_trait]
impl TalosAgent for TalosAgentImplV2 {
    async fn certify(&self, request: CertificationRequest) -> Result<CertificationResponse, String> {
        let (tx, mut rx) = mpsc::channel::<CertificationResponse>(1);

        let m = CertifyRequestChannelMessage::new(&request, &tx);

        let to_state_manager = self.rx_certify.clone();
        let response = match to_state_manager.send(m).await {
            Ok(()) => match rx.recv().await {
                Some(mut response) => {
                    response.received_at = OffsetDateTime::now_utc().unix_timestamp_nanos() as u64;
                    Ok(response)
                }
                None => Err("No response received".to_string()),
            },

            Err(e) => Err(format!("No response received: {:?}", e.to_string())),
        };

        response
    }
}
