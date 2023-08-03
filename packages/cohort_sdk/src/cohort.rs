use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use metrics::opentel::global;
use opentelemetry_api::metrics::{Counter, Histogram, Unit};
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
use tokio::sync::mpsc;

use crate::{
    delay_controller::DelayController,
    installer_callback::ReplicatorInstallerImpl,
    model::{
        self,
        callbacks::{ItemStateProvider, OutOfOrderInstallOutcome, OutOfOrderInstaller, StatemapInstaller},
        internal::CertificationAttemptOutcome,
        CertificationResponse, ClientError, Config, ReplicatorServices, ResponseMetadata,
    },
    replicator2::{cohort_replicator::CohortReplicator, cohort_suffix::CohortSuffix, service::ReplicatorService2},
};

use talos_certifier::ports::MessageReciever;

use talos_agent::api::CertificationResponse as InternalCertificationResponse;

// #[napi]
pub struct Cohort {
    config: Config,
    talos_agent: Box<dyn TalosAgent + Sync + Send>,
    agent_services: AgentServices,
    replicator_services: ReplicatorServices,
    oo_retry_counter: Arc<Counter<u64>>,
    oo_giveups_counter: Arc<Counter<u64>>,
    oo_not_safe_counter: Arc<Counter<u64>>,
    oo_install_histogram: Arc<Histogram<f64>>,
    oo_attempts_histogram: Arc<Histogram<u64>>,
    oo_install_and_wait_histogram: Arc<Histogram<f64>>,
    oo_wait_histogram: Arc<Histogram<f64>>,
    talos_histogram: Arc<Histogram<f64>>,
    talos_aborts_counter: Arc<Counter<u64>>,
    agent_retries_counter: Arc<Counter<u64>>,
    agent_errors_counter: Arc<Counter<u64>>,
    db_errors_counter: Arc<Counter<u64>>,
}

// #[napi]
impl Cohort {
    // #[napi]
    pub async fn create<S>(
        config: Config,
        // Param1: The list of statemap items.
        // Param2: Version to install.
        // Returns error descrition. If string is empty it means there was no error installing
        statemap_installer: S,
    ) -> Result<Self, ClientError>
    where
        S: StatemapInstaller + Sync + Send + 'static,
    {
        let agent_config: AgentConfig = config.clone().into();
        let kafka_config: KafkaConfig = config.clone().into();
        let talos_kafka_config: TalosKafkaConfig = config.clone().into();

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
        // Code below is to start replicator from master branch...
        //

        //
        // start replicator
        //

        let suffix = CohortSuffix::with_config(config.clone().into());
        let kafka_consumer = KafkaConsumer::new(&talos_kafka_config);
        kafka_consumer.subscribe().await.unwrap();
        let replicator = CohortReplicator::new(kafka_consumer, suffix);

        let (tx_install_req, rx_statemaps_ch) = mpsc::channel(config.replicator_buffer_size);
        let (tx_install_result_ch, rx_install_result) = tokio::sync::mpsc::channel(config.replicator_buffer_size);
        let replicator_handle = tokio::spawn(ReplicatorService2::start_replicator(replicator, tx_install_req, rx_install_result));
        let replicator_impl = ReplicatorInstallerImpl {
            installer_impl: statemap_installer,
        };
        let installer_handle = tokio::spawn(ReplicatorService2::start_installer(rx_statemaps_ch, tx_install_result_ch, replicator_impl));

        let meter = global::meter("cohort_sdk");
        let oo_install_histogram = meter.f64_histogram("metric_oo_install_duration").with_unit(Unit::new("ms")).init();
        let oo_attempts_histogram = meter.u64_histogram("metric_oo_attempts").with_unit(Unit::new("tx")).init();
        let oo_install_and_wait_histogram = meter.f64_histogram("metric_oo_install_and_wait_duration").with_unit(Unit::new("ms")).init();
        let oo_wait_histogram = meter.f64_histogram("metric_oo_wait_duration").with_unit(Unit::new("ms")).init();
        let talos_histogram = meter.f64_histogram("metric_talos").with_unit(Unit::new("ms")).init();
        let oo_retry_counter = meter.u64_counter("metric_oo_retry_count").with_unit(Unit::new("tx")).init();
        let oo_giveups_counter = meter.u64_counter("metric_oo_giveups_count").with_unit(Unit::new("tx")).init();
        let oo_not_safe_counter = meter.u64_counter("metric_oo_not_safe_count").with_unit(Unit::new("tx")).init();
        let talos_aborts_counter = meter.u64_counter("metric_talos_aborts_count").with_unit(Unit::new("tx")).init();
        let agent_errors_counter = meter.u64_counter("metric_agent_errors_count").with_unit(Unit::new("tx")).init();
        let agent_retries_counter = meter.u64_counter("metric_agent_retries_count").with_unit(Unit::new("tx")).init();
        let db_errors_counter = meter.u64_counter("metric_db_errors_count").with_unit(Unit::new("tx")).init();

        Ok(Self {
            config,
            talos_agent: Box::new(agent),
            agent_services,
            replicator_services: ReplicatorServices {
                replicator_handle,
                installer_handle,
            },
            oo_install_histogram: Arc::new(oo_install_histogram),
            oo_install_and_wait_histogram: Arc::new(oo_install_and_wait_histogram),
            oo_wait_histogram: Arc::new(oo_wait_histogram),
            oo_retry_counter: Arc::new(oo_retry_counter),
            oo_giveups_counter: Arc::new(oo_giveups_counter),
            oo_not_safe_counter: Arc::new(oo_not_safe_counter),
            oo_attempts_histogram: Arc::new(oo_attempts_histogram),
            talos_histogram: Arc::new(talos_histogram),
            talos_aborts_counter: Arc::new(talos_aborts_counter),
            agent_retries_counter: Arc::new(agent_retries_counter),
            agent_errors_counter: Arc::new(agent_errors_counter),
            db_errors_counter: Arc::new(db_errors_counter),
        })
    }

    pub async fn certify<S, O>(
        &self,
        request: model::CertificationRequest,
        state_provider: &S,
        oo_installer: &O,
    ) -> Result<model::CertificationResponse, ClientError>
    where
        S: ItemStateProvider,
        O: OutOfOrderInstaller,
    {
        let span_1 = Instant::now();
        let response = self.send_to_talos(request, state_provider).await?;
        let span_1_val = span_1.elapsed().as_nanos() as f64 / 1_000_000_f64;

        let h_talos = Arc::clone(&self.talos_histogram);
        tokio::spawn(async move {
            let scale = global::scaling_config().get_scale_factor("metric_talos");
            h_talos.record(span_1_val * scale as f64, &[]);
        });

        if response.decision == Decision::Aborted {
            return Ok(response);
        }

        // system error if we have Commit decision but no safepoint is given
        let safepoint = response.safepoint.unwrap();
        let new_version = response.version;

        let mut controller = DelayController::new(20, self.config.retry_oo_backoff_max_ms);
        let mut attempt = 0;
        let span_2 = Instant::now();

        let mut is_not_save = 0_u64;
        let mut giveups = 0_u64;

        let result = loop {
            attempt += 1;

            let span_3 = Instant::now();
            let install_result = oo_installer.install(response.xid.clone(), safepoint, new_version, attempt).await;
            let span_3_val = span_3.elapsed().as_nanos() as f64 / 1_000_000_f64;

            let h_install = Arc::clone(&self.oo_install_histogram);

            tokio::spawn(async move {
                let scale = global::scaling_config().get_scale_factor("metric_oo_install_duration");
                h_install.record(span_3_val * scale as f64, &[]);
            });

            let error = match install_result {
                Ok(OutOfOrderInstallOutcome::Installed) => None,
                Ok(OutOfOrderInstallOutcome::InstalledAlready) => None,
                Ok(OutOfOrderInstallOutcome::SafepointCondition) => {
                    is_not_save += 1;
                    // We create this error as "safepoint timeout" in advance. Error is erased if further attempt will be successfull or replaced with anotuer error.
                    Some(ClientError {
                        kind: model::ClientErrorKind::OutOfOrderSnapshotTimeout,
                        reason: format!("Timeout waitig for safepoint: {}", safepoint),
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
                    giveups += 1;
                    break Err(client_error);
                }

                // try again
                controller.sleep().await;
            } else {
                break Ok(response);
            }
        };

        let span_2_val = span_2.elapsed().as_nanos() as f64 / 1_000_000_f64;
        let total_sleep = controller.total_sleep_time;

        let c_not_safe = Arc::clone(&self.oo_not_safe_counter);
        let h_total_sleep = Arc::clone(&self.oo_wait_histogram);
        let h_attempts = Arc::clone(&self.oo_attempts_histogram);
        let h_span_2 = Arc::clone(&self.oo_install_and_wait_histogram);
        let c_giveups = Arc::clone(&self.oo_giveups_counter);
        let c_retry = Arc::clone(&self.oo_retry_counter);

        tokio::spawn(async move {
            if is_not_save > 0 {
                c_not_safe.add(is_not_save, &[]);
            }
            if total_sleep > 0 {
                let scale = global::scaling_config().get_scale_factor("metric_oo_wait_duration");
                h_total_sleep.record(total_sleep as f64 * scale as f64, &[]);
            }
            if giveups > 0 {
                c_giveups.add(giveups, &[]);
            }
            if attempt > 1 {
                c_retry.add(attempt - 1, &[]);
            }

            h_attempts.record(attempt, &[]);
            let scale = global::scaling_config().get_scale_factor("metric_oo_install_and_wait_duration");
            h_span_2.record(span_2_val * scale as f64, &[]);
        });
        result
    }

    async fn send_to_talos<S>(&self, request: model::CertificationRequest, state_provider: &S) -> Result<model::CertificationResponse, ClientError>
    where
        S: ItemStateProvider,
    {
        let started_at = Instant::now();
        let mut attempts = 0;

        // let mut delay_controller = Box::new(DelayController::new(20, self.config.retry_backoff_max_ms));
        let mut delay_controller = DelayController::new(20, self.config.retry_backoff_max_ms);
        let mut talos_aborts = 0_u64;
        let mut agent_errors = 0_u64;
        let mut db_errors = 0_u64;

        let result = loop {
            // One of these will be sent to client if we failed
            let recent_error: Option<ClientError>;
            let recent_response: Option<CertificationResponse>;

            attempts += 1;
            let is_success = match self.send_to_talos_attempt(request.clone(), state_provider).await {
                CertificationAttemptOutcome::Success { mut response } => {
                    response.metadata.duration_ms = started_at.elapsed().as_millis() as u64;
                    response.metadata.attempts = attempts;
                    recent_error = None;
                    recent_response = Some(response);
                    true
                }
                CertificationAttemptOutcome::Aborted { mut response } => {
                    talos_aborts += 1;
                    response.metadata.duration_ms = started_at.elapsed().as_millis() as u64;
                    response.metadata.attempts = attempts;
                    recent_error = None;
                    recent_response = Some(response);
                    false
                }
                CertificationAttemptOutcome::AgentError { error } => {
                    recent_error = Some(ClientError::from(error));
                    recent_response = None;
                    agent_errors += 1;
                    false
                }
                CertificationAttemptOutcome::DataError { reason } => {
                    recent_error = Some(ClientError {
                        kind: model::ClientErrorKind::Persistence,
                        reason,
                        cause: None,
                    });
                    recent_response = None;
                    db_errors += 1;
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
            } else if let Some(response) = recent_response {
                log::debug!(
                    "Unsuccessful transaction: {:?}. Response: {:?} This might retry. Attempts: {}",
                    request.candidate.statemap,
                    response.decision,
                    attempts
                );
            } else if let Some(error) = recent_error {
                log::debug!(
                    "Unsuccessful transaction with error: {:?}. {} This might retry. Attempts: {}",
                    request.candidate.statemap,
                    error,
                    attempts
                );
            }

            delay_controller.sleep().await;
        };

        let c_talos_aborts = Arc::clone(&self.talos_aborts_counter);
        let c_agent_retries = Arc::clone(&self.agent_retries_counter);
        let c_agent_errors = Arc::clone(&self.agent_errors_counter);
        let c_db_errors = Arc::clone(&self.db_errors_counter);

        if agent_errors > 0 || db_errors > 0 || attempts > 1 || talos_aborts > 0 {
            tokio::spawn(async move {
                c_talos_aborts.add(talos_aborts, &[]);
                c_agent_retries.add(attempts, &[]);
                c_agent_errors.add(agent_errors, &[]);
                c_db_errors.add(db_errors, &[]);
            });
        }

        result
    }

    async fn send_to_talos_attempt<S>(&self, request: model::CertificationRequest, state_provider: &S) -> CertificationAttemptOutcome
    where
        S: ItemStateProvider,
    {
        let result_local_state = state_provider.get_state().await;
        if let Err(reason) = result_local_state {
            return CertificationAttemptOutcome::DataError { reason };
        }

        let local_state = result_local_state.unwrap();

        log::debug!("loaded state: {}, {:?}", local_state.snapshot_version, local_state.items);

        let (snapshot, readvers) = Self::select_snapshot_and_readvers(local_state.snapshot_version, local_state.items.iter().map(|i| i.version).collect());

        let xid = uuid::Uuid::new_v4().to_string();
        let agent_request = CertificationRequest {
            message_key: xid.clone(),
            candidate: CandidateData {
                xid: xid.clone(),
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
            Err(error) => CertificationAttemptOutcome::AgentError { error },
        }
    }

    fn select_snapshot_and_readvers(cpt_snapshot: u64, cpt_versions: Vec<u64>) -> (u64, Vec<u64>) {
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
