use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use cohort::{
    delay_controller::DelayController,
    replicator::{
        core::{Replicator, ReplicatorCandidate, ReplicatorChannel, StatemapItem},
        services::{replicator_service::replicator_service, statemap_installer_service::installer_service},
    },
};
use futures::future::BoxFuture;
use talos_agent::{
    agent::{
        core::{AgentServices, TalosAgentImpl},
        model::{CancelRequestChannelMessage, CertifyRequestChannelMessage},
    },
    api::{AgentConfig, CandidateData, CertificationRequest, KafkaConfig, TalosAgent},
    messaging::{
        api::{Decision, DecisionMessage},
        kafka::KafkaInitializer,
    },
    metrics::{client::MetricsClient, model::Signal},
    mpsc::core::{ReceiverWrapper, SenderWrapper},
};

use talos_certifier_adapters::{kafka::config::KafkaConfig as TalosKafkaConfig, KafkaConsumer};
use talos_suffix::Suffix;
use tokio::sync::mpsc;

use crate::{
    installer_callback::ReplicatorInstallerImpl,
    model::{
        self,
        callbacks::{CapturedState, GetStateFunction, InstallerFunction, OutOfOrderInstallerFunction},
        internal::CertificationAttemptOutcome,
        CertificationResponse, ClientError, Config, ReplicatorServices, ResponseMetadata,
    },
};

use talos_certifier::ports::MessageReciever;

use talos_agent::api::CertificationResponse as InternalCertificationResponse;

// #[napi]
pub struct Cohort {
    config: Config,
    // database: Arc<Database>,
    talos_agent: Box<dyn TalosAgent + Sync + Send>,
    agent_services: AgentServices,
    replicator_services: ReplicatorServices,
}

// #[napi]
impl Cohort {
    // #[napi]
    pub async fn create(
        config: Config,
        // Param1: The list of statemap items.
        // Param2: Version to install.
        // Returns error descrition. If string is empty it means there was no error installing
        installer_function: InstallerFunction,
    ) -> Result<Self, ClientError> {
        let agent_config: AgentConfig = config.clone().into();
        let kafka_config: KafkaConfig = config.clone().into();
        let talos_kafka_config: TalosKafkaConfig = config.clone().into();
        // let db_config: DatabaseConfig = config.clone().into();

        // let database = Database::init_db(db_config).await.map_err(|e| ClientError::from(e))?;

        //
        // Create instance of Agent
        //
        let (tx_certify_ch, rx_certify_ch) = tokio::sync::mpsc::channel::<CertifyRequestChannelMessage>(agent_config.buffer_size);
        let tx_certify = SenderWrapper::<CertifyRequestChannelMessage> { tx: tx_certify_ch };
        let rx_certify = ReceiverWrapper::<CertifyRequestChannelMessage> { rx: rx_certify_ch };

        let (tx_cancel_ch, rx_cancel_ch) = tokio::sync::mpsc::channel::<CancelRequestChannelMessage>(agent_config.buffer_size);
        let tx_cancel = SenderWrapper::<CancelRequestChannelMessage> { tx: tx_cancel_ch };
        let rx_cancel = ReceiverWrapper::<CancelRequestChannelMessage> { rx: rx_cancel_ch };

        let (tx_decision_ch, rx_decision_ch) = tokio::sync::mpsc::channel::<DecisionMessage>(agent_config.buffer_size);
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

        let (publisher, consumer) = KafkaInitializer::connect(agent_config.agent.clone(), kafka_config)
            .await
            .map_err(|me| ClientError {
                kind: model::ClientErrorKind::Messaging,
                reason: "Error connecting Talos agent to Kafka.".into(),
                cause: Some(me.reason),
            })?;

        let agent_services = agent.start(rx_certify, rx_cancel, tx_decision, rx_decision, publisher, consumer);

        //
        // start replicator
        //

        let suffix: Suffix<ReplicatorCandidate> = Suffix::with_config(config.clone().into());

        let kafka_consumer = KafkaConsumer::new(&talos_kafka_config);
        kafka_consumer.subscribe().await.unwrap();

        let replicator = Replicator::new(kafka_consumer, suffix);
        let (tx_statemaps_ch, rx_statemaps_ch) = mpsc::channel::<(Vec<StatemapItem>, Option<u64>)>(config.replicator_buffer_size);
        let (tx_install_result_ch, rx_install_result) = mpsc::channel::<ReplicatorChannel>(config.replicator_buffer_size);

        let replicator_handle = tokio::spawn(replicator_service(tx_statemaps_ch, rx_install_result, replicator));
        let replicator_impl = ReplicatorInstallerImpl {
            external_impl: installer_function,
        };

        let installer_handle = tokio::spawn(installer_service(rx_statemaps_ch, tx_install_result_ch, replicator_impl));

        Ok(Self {
            config,
            // database,
            talos_agent: Box::new(agent),
            agent_services,
            replicator_services: ReplicatorServices {
                replicator_handle,
                installer_handle,
            },
        })
    }

    pub async fn certify(
        &self,
        request: model::CertificationRequest,
        get_state_function: GetStateFunction,
        installer_function: OutOfOrderInstallerFunction,
    ) -> Result<model::CertificationResponse, ClientError> {
        let response = self.send_to_talos(request, get_state_function).await?;

        if response.decision == Decision::Aborted {
            return Ok(response);
        }

        // system error if we have Commit decision but no safepoint is given
        let safepoint = response.safepoint.unwrap();

        // await until safe, then install out of order
        let mut controller = DelayController::new(self.config.retry_oo_backoff_max_ms);
        let installer_function = Arc::new(installer_function);
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut error = installer_function(response.xid.clone(), safepoint, attempt).await;
            error = error.trim().into();

            if error.is_empty() {
                return Ok(response);
            }

            if attempt >= self.config.retry_oo_attempts_max {
                return Err(ClientError {
                    kind: model::ClientErrorKind::OutOfOrderCallbackFailed,
                    reason: error,
                    cause: None,
                });
            }

            controller.sleep().await;
        }
    }

    async fn send_to_talos(
        &self,
        request: model::CertificationRequest,
        get_state_function: Box<dyn Fn() -> BoxFuture<'static, CapturedState> + Sync + Send>,
    ) -> Result<model::CertificationResponse, ClientError> {
        let started_at = Instant::now();

        let get_state_function = Arc::new(get_state_function);
        let mut attempts = 0;

        let mut delay_controller = Box::new(DelayController::new(self.config.retry_backoff_max_ms));
        loop {
            // One of these will be sent to client if we failed
            let recent_error: Option<ClientError>;
            let recent_response: Option<CertificationResponse>;

            attempts += 1;
            let is_success = match self.send_to_talos_attempt(request.clone(), Arc::clone(&get_state_function)).await {
                CertificationAttemptOutcome::Success { mut response } => {
                    response.metadata.duration_ms = Instant::now().duration_since(started_at).as_millis() as u64;
                    response.metadata.attempts = attempts;
                    recent_error = None;
                    recent_response = Some(response);
                    true
                }
                CertificationAttemptOutcome::Aborted { mut response } => {
                    response.metadata.duration_ms = Instant::now().duration_since(started_at).as_millis() as u64;
                    response.metadata.attempts = attempts;
                    recent_error = None;
                    recent_response = Some(response);
                    false
                }
                CertificationAttemptOutcome::Error { error } => {
                    recent_error = Some(ClientError::from(error));
                    recent_response = None;
                    false
                }
            };

            if is_success {
                break Ok(recent_response.unwrap());
            }

            if self.config.retry_attempts_max <= attempts {
                if let Some(response) = recent_response {
                    break Ok(response);
                } else if let Some(error) = recent_error {
                    break Err(error);
                }
            }

            delay_controller.sleep().await;
        }
    }

    async fn send_to_talos_attempt(
        &self,
        request: model::CertificationRequest,
        get_state_function: Arc<Box<dyn Fn() -> BoxFuture<'static, CapturedState> + Sync + Send>>,
    ) -> CertificationAttemptOutcome {
        let local_state = get_state_function().await;

        let (snapshot, readvers) = Self::select_snapshot_and_readvers(local_state.snapshot_version, local_state.items.iter().map(|i| i.version).collect());

        let agent_request = CertificationRequest {
            message_key: request.xid,
            candidate: CandidateData {
                xid: request.candidate.xid,
                statemap: request.candidate.statemap,
                readset: request.candidate.readset,
                writeset: request.candidate.writeset,
                readvers,
                snapshot,
            },
            timeout: if request.timeout_ms > 0 {
                Some(Duration::from_millis(request.timeout_ms))
            } else {
                None
            },
        };

        match self.talos_agent.certify(agent_request).await {
            Ok(agent_response) => {
                let response = CertificationResponse {
                    xid: agent_response.xid,
                    decision: agent_response.decision,
                    safepoint: agent_response.safepoint,
                    version: agent_response.version,
                    metadata: ResponseMetadata { duration_ms: 0, attempts: 0 },
                };

                if response.decision == Decision::Aborted {
                    CertificationAttemptOutcome::Aborted { response }
                } else {
                    CertificationAttemptOutcome::Success { response }
                }
            }
            Err(error) => CertificationAttemptOutcome::Error { error },
        }
    }

    fn select_snapshot_and_readvers(cpt_snapshot: u64, cpt_versions: Vec<u64>) -> (u64, Vec<u64>) {
        log::debug!("select_snapshot_and_readvers({}, {:?})", cpt_snapshot, cpt_versions);
        if cpt_versions.is_empty() {
            log::debug!(
                "select_snapshot_and_readvers({}, {:?}): {:?}",
                cpt_snapshot,
                cpt_versions,
                (cpt_snapshot, Vec::<u64>::new())
            );
            return (cpt_snapshot, vec![]);
        }

        let mut cpt_version_min: u64 = u64::MAX;
        for v in cpt_versions.iter() {
            if cpt_version_min > *v {
                cpt_version_min = *v;
            }
        }
        let snapshot_version = std::cmp::max(cpt_snapshot, cpt_version_min);
        let mut read_vers = Vec::<u64>::new();
        for v in cpt_versions.iter() {
            if snapshot_version < *v {
                read_vers.push(*v);
            }
        }

        log::debug!(
            "select_snapshot_and_readvers({}, {:?}): {:?}",
            cpt_snapshot,
            cpt_versions,
            (snapshot_version, read_vers.clone())
        );
        (snapshot_version, read_vers)
    }

    pub async fn shutdown(&self) {
        // TODO implement graceful shutdown with timeout? Wait for channels to be drained and then exit.
        // while self.channel_tx_certify.capacity() != MAX { wait() }
        self.agent_services.decision_reader.abort();
        self.agent_services.state_manager.abort();
        self.replicator_services.replicator_handle.abort();
        self.replicator_services.installer_handle.abort();
    }
}
