use std::sync::Arc;

use talos_agent::{
    agent::{
        core::{AgentServices, TalosAgentImpl},
        model::{CancelRequestChannelMessage, CertifyRequestChannelMessage},
    },
    api::{AgentConfig, TalosType},
    messaging::{
        api::{Decision, DecisionMessage},
        kafka::KafkaInitializer,
    },
    metrics::{client::MetricsClient, model::Signal},
    mpsc::core::{ReceiverWrapper, SenderWrapper},
};
use talos_rdkafka_utils::kafka_config::KafkaConfig;

use crate::{
    delay_controller::DelayController,
    model::{
        self,
        callbacks::{ItemStateProvider, OutOfOrderInstallOutcome, OutOfOrderInstaller},
        CertificationResponse, ClientError, Config, ResponseMetadata,
    },
};

use talos_agent::api::CertificationResponse as InternalCertificationResponse;

pub struct CohortMock {
    config: Config,
    agent_services: AgentServices,
}

impl CohortMock {
    pub async fn create(
        config: Config,
        // Param1: The list of statemap items.
        // Param2: Version to install.
        // Returns error descrition. If string is empty it means there was no error installing
    ) -> Result<Self, ClientError> {
        let agent_config: AgentConfig = config.clone().into();
        let kafka_config: KafkaConfig = config.kafka.clone();

        //
        // Create instance of Agent
        //
        let (tx_certify_ch, rx_certify_ch) = tokio::sync::mpsc::channel::<CertifyRequestChannelMessage>(agent_config.buffer_size as usize);
        let tx_certify = SenderWrapper::<CertifyRequestChannelMessage> { tx: tx_certify_ch };
        let rx_certify = ReceiverWrapper::<CertifyRequestChannelMessage> { rx: rx_certify_ch };

        let (tx_cancel_ch, rx_cancel_ch) = tokio::sync::mpsc::channel::<CancelRequestChannelMessage>(agent_config.buffer_size as usize);
        let tx_cancel = SenderWrapper::<CancelRequestChannelMessage> { tx: tx_cancel_ch };
        let rx_cancel = ReceiverWrapper::<CancelRequestChannelMessage> { rx: rx_cancel_ch };

        let (tx_decision_ch, rx_decision_ch) = tokio::sync::mpsc::channel::<DecisionMessage>(agent_config.buffer_size as usize);
        let tx_decision = SenderWrapper::<DecisionMessage> { tx: tx_decision_ch };
        let rx_decision = ReceiverWrapper::<DecisionMessage> { rx: rx_decision_ch };

        let metrics_client: Option<Box<MetricsClient<SenderWrapper<Signal>>>> = None;

        let agent = TalosAgentImpl::new(
            agent_config.clone(),
            Arc::new(Box::new(tx_certify)),
            tx_cancel,
            None,
            Arc::new(metrics_client),
            || {
                let (tx_ch, rx_ch) = tokio::sync::mpsc::channel::<InternalCertificationResponse>(1);
                (SenderWrapper { tx: tx_ch }, ReceiverWrapper { rx: rx_ch })
            },
        );

        let (publisher, consumer) = KafkaInitializer::connect(agent_config.agent.clone(), kafka_config, TalosType::External)
            .await
            .map_err(|me| ClientError {
                kind: model::ClientErrorKind::Messaging,
                reason: "Error connecting Talos agent to Kafka.".into(),
                cause: Some(me.reason),
            })?;

        let agent_services = agent.start(rx_certify, rx_cancel, tx_decision, rx_decision, publisher, consumer);

        println!("cohort created");
        Ok(Self { config, agent_services })
    }

    pub async fn certify<S, O>(
        &self,
        _request: model::CertificationRequest,
        state_provider: &S,
        oo_installer: &O,
    ) -> Result<model::CertificationResponse, ClientError>
    where
        S: ItemStateProvider,
        O: OutOfOrderInstaller,
    {
        // println!("CohortMock.certify(): Before state_provider.get_state()");
        let _state = state_provider.get_state().await.map_err(|e| ClientError {
            kind: model::ClientErrorKind::Internal,
            reason: format!("Cannot load state: {}.", e),
            cause: Some(e),
        })?;

        // println!("CohortMock.certify(): After state_provider.get_state(). Loaded: {:?}", state);

        let response = CertificationResponse {
            safepoint: Some(2),
            xid: "mock-xid".to_string(),
            version: 2,
            metadata: ResponseMetadata { attempts: 2, duration_ms: 222 },
            decision: Decision::Committed,
            conflict: None,
        };

        if response.decision == Decision::Aborted {
            return Ok(response);
        }

        // system error if we have Commit decision but no safepoint is given
        let safepoint = response.safepoint.unwrap();
        let new_version = response.version;

        let mut controller = DelayController::new(self.config.retry_oo_backoff.min_ms, self.config.retry_oo_backoff.max_ms);
        let mut attempt = 0;

        loop {
            attempt += 1;

            // println!("CohortMock.certify(): before 'oo_installer.install()'");
            let install_result = oo_installer.install(response.xid.clone(), safepoint, new_version, attempt).await;
            // println!("CohortMock.certify(): after 'oo_installer.install()': {:?}", install_result);
            let error = match install_result {
                Ok(OutOfOrderInstallOutcome::Installed) => None,
                Ok(OutOfOrderInstallOutcome::InstalledAlready) => None,
                Ok(OutOfOrderInstallOutcome::SafepointCondition) => {
                    // We create this error as "safepoint timeout" in advance. Error is erased if further attempt will be successfull or replaced with anotuer error.
                    Some(ClientError {
                        kind: model::ClientErrorKind::OutOfOrderSnapshotTimeout,
                        reason: format!("Timeout waiting for safepoint: {}", safepoint),
                        cause: None,
                    })
                }
                Err(error) => Some(ClientError {
                    kind: model::ClientErrorKind::OutOfOrderCallbackFailed,
                    reason: error,
                    cause: None,
                }),
            };

            if let Some(client_error) = error {
                if attempt >= self.config.retry_oo_attempts_max {
                    break Err(client_error);
                }

                // try again
                controller.sleep().await;
            } else {
                break Ok(response);
            }
        }
    }

    pub async fn shutdown(&self) {
        self.agent_services.decision_reader.abort();
        self.agent_services.state_manager.abort();
    }
}
