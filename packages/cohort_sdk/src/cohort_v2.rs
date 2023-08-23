use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use futures::Future;
use opentelemetry_api::{
    global,
    metrics::{Counter, Histogram, Unit},
};
use talos_agent::{
    agent::{
        core::{AgentServices, TalosAgentImpl},
        model::{CancelRequestChannelMessage, CertifyRequestChannelMessage},
    },
    api::{AgentConfig, CandidateData, CertificationRequest, TalosAgent, TalosType},
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
        callbacks::{CohortCapturedState, OutOfOrderInstallOutcome, OutOfOrderInstaller},
        internal::CertificationAttemptOutcome,
        CertificationResponse, ClientError, Config, ResponseMetadata,
    },
};

use talos_agent::api::CertificationResponse as InternalCertificationResponse;

// #[napi]
pub struct Cohort {
    config: Config,
    talos_agent: Box<dyn TalosAgent + Sync + Send>,
    agent_services: AgentServices,
    oo_retry_counter: Arc<Counter<u64>>,
    oo_giveups_counter: Arc<Counter<u64>>,
    oo_not_safe_counter: Arc<Counter<u64>>,
    oo_install_histogram: Arc<Histogram<f64>>,
    oo_attempts_histogram: Arc<Histogram<u64>>,
    oo_install_and_wait_histogram: Arc<Histogram<f64>>,
    oo_wait_histogram: Arc<Histogram<f64>>,
    talos_histogram: Arc<Histogram<f64>>,
    talos_aborts_counter: Arc<Counter<u64>>,
    agent_retries_histogram: Arc<Histogram<u64>>,
    agent_errors_counter: Arc<Counter<u64>>,
    db_errors_counter: Arc<Counter<u64>>,
}

// #[napi]
impl Cohort {
    // #[napi]
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

        let (publisher, consumer) = KafkaInitializer::connect(agent_config.agent.clone(), kafka_config, TalosType::External)
            .await
            .map_err(|me| ClientError {
                kind: model::ClientErrorKind::Messaging,
                reason: "Error connecting Talos agent to Kafka.".into(),
                cause: Some(me.reason),
            })?;

        let agent_services = agent.start(rx_certify, rx_cancel, tx_decision, rx_decision, publisher, consumer);

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
        let agent_retries_histogram = meter.u64_histogram("metric_agent_retries").with_unit(Unit::new("tx")).init();
        let db_errors_counter = meter.u64_counter("metric_db_errors_count").with_unit(Unit::new("tx")).init();

        oo_retry_counter.add(0, &[]);
        oo_giveups_counter.add(0, &[]);
        oo_not_safe_counter.add(0, &[]);
        talos_aborts_counter.add(0, &[]);
        agent_errors_counter.add(0, &[]);
        db_errors_counter.add(0, &[]);

        Ok(Self {
            config,
            talos_agent: Box::new(agent),
            agent_services,
            oo_install_histogram: Arc::new(oo_install_histogram),
            oo_install_and_wait_histogram: Arc::new(oo_install_and_wait_histogram),
            oo_wait_histogram: Arc::new(oo_wait_histogram),
            oo_retry_counter: Arc::new(oo_retry_counter),
            oo_giveups_counter: Arc::new(oo_giveups_counter),
            oo_not_safe_counter: Arc::new(oo_not_safe_counter),
            oo_attempts_histogram: Arc::new(oo_attempts_histogram),
            talos_histogram: Arc::new(talos_histogram),
            agent_retries_histogram: Arc::new(agent_retries_histogram),
            talos_aborts_counter: Arc::new(talos_aborts_counter),
            agent_errors_counter: Arc::new(agent_errors_counter),
            db_errors_counter: Arc::new(db_errors_counter),
        })
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
        self.agent_services.decision_reader.abort();
        self.agent_services.state_manager.abort();
    }

    pub async fn certify<O, F, Fut>(&self, get_payload_and_snapshot: &F, oo_installer: &O) -> Result<model::CertificationResponse, ClientError>
    where
        O: OutOfOrderInstaller,

        F: Fn() -> Fut,
        Fut: Future<Output = Result<CohortCapturedState, String>>,
    {
        // 1. Get the snapshot
        // 2. Send for certification
        let span_1 = Instant::now();
        let response = self.create_request_and_certify(get_payload_and_snapshot).await?;
        let span_1_val = span_1.elapsed().as_nanos() as f64 / 1_000_000_f64;

        let h_talos = Arc::clone(&self.talos_histogram);
        tokio::spawn(async move {
            h_talos.record(span_1_val * 100.0, &[]);
        });

        if response.decision == Decision::Aborted {
            return Ok(response);
        }

        // 3. OOO install
        self.install_success_response(response, oo_installer).await
    }

    pub async fn install_success_response<O>(&self, response: CertificationResponse, oo_installer: &O) -> Result<model::CertificationResponse, ClientError>
    where
        O: OutOfOrderInstaller,
    {
        let safepoint = response.safepoint.unwrap();
        let new_version = response.version;

        let mut controller = DelayController::new(self.config.retry_oo_backoff.min_ms, self.config.retry_oo_backoff.max_ms);
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
                h_install.record(span_3_val * 100.0, &[]);
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
                h_total_sleep.record(total_sleep as f64 * 100.0, &[]);
            }
            if giveups > 0 {
                c_giveups.add(giveups, &[]);
            }
            if attempt > 1 {
                c_retry.add(attempt as u64 - 1, &[]);
            }

            h_attempts.record(attempt as u64, &[]);
            h_span_2.record(span_2_val * 100.0, &[]);
        });
        result
    }

    pub async fn create_request_and_certify<F, Fut>(&self, get_payload_and_snapshot: &F) -> Result<model::CertificationResponse, ClientError>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<CohortCapturedState, String>>,
    {
        let started_at = Instant::now();
        let mut result: Option<Result<CertificationResponse, ClientError>> = None;

        let mut delay_controller = DelayController::new(self.config.retry_backoff.min_ms, self.config.retry_backoff.max_ms);

        let mut attempts = 0;
        let mut talos_aborts = 0_u64;
        let mut agent_errors = 0_u64;
        let mut db_errors = 0_u64;

        let mut recent_conflict: Option<u64> = None;
        let mut recent_abort: Option<CertificationResponse> = None;

        let k = loop {
            let timeout = Duration::from_millis(self.config.snapshot_wait_timeout_ms as u64);
            let res_request = self.await_for_snapshot(&get_payload_and_snapshot, recent_conflict, timeout).await;

            let request = match res_request {
                Ok(CohortCapturedState::Proceed(request)) => request,
                // User initiated abort
                Ok(CohortCapturedState::Abort(reason)) => {
                    break Err(ClientError {
                        kind: model::ClientErrorKind::ClientAborted,
                        reason,
                        cause: None,
                    })
                }
                // Timeout waiting for snapshot
                Err(SnapshotPollErrorType::Timeout { waited }) => {
                    log::error!("Timeout wating for snapshot: {:?}. Waited: {:.2} sec", recent_conflict, waited.as_secs_f32());

                    break Ok(recent_abort.unwrap());
                }
                Err(SnapshotPollErrorType::FetchError { reason }) => {
                    db_errors += 1;

                    attempts += 1;
                    if attempts >= self.config.retry_attempts_max {
                        break Err(ClientError {
                            kind: model::ClientErrorKind::Persistence,
                            reason,
                            cause: None,
                        });
                    } else {
                        continue;
                    }
                }
            };

            // Send to talos
            match self.send_to_talos_agent(request).await {
                CertificationAttemptOutcome::Success { mut response } => {
                    response.metadata.duration_ms = started_at.elapsed().as_millis() as u64;
                    response.metadata.attempts = attempts;
                    break Ok(response);
                }
                CertificationAttemptOutcome::Aborted { mut response } => {
                    talos_aborts += 1;
                    response.metadata.duration_ms = started_at.elapsed().as_millis() as u64;
                    response.metadata.attempts = attempts;
                    recent_conflict = response.conflict;
                    recent_abort = Some(response.clone());
                    // result = recent_abort.map(|a| Ok(a));
                    result = Some(Ok(response));
                }
                CertificationAttemptOutcome::AgentError { error } => {
                    result = Some(Err(ClientError::from(error)));
                    agent_errors += 1;
                }
                _ => {} //
                        // CertificationAttemptOutcome::ClientAborted { reason } => {
                        //     result = Some(Err(ClientError {
                        //         kind: model::ClientErrorKind::ClientAborted,
                        //         reason,
                        //         cause: None,
                        //     }));
                        // }
                        // CertificationAttemptOutcome::SnapshotTimeout { waited, conflict } => {
                        //     log::error!("Timeout wating for snapshot: {:?}. Waited: {:.2} sec", conflict, waited.as_secs_f32());
                        //     result = recent_abort.clone().map(Ok);
                        // }
                        // CertificationAttemptOutcome::DataError { reason } => {
                        //     result = Some(Err(ClientError {
                        //         kind: model::ClientErrorKind::Persistence,
                        //         reason,
                        //         cause: None,
                        //     }));
                        //     db_errors += 1;
                        // }
            }

            attempts += 1;
            if attempts >= self.config.retry_attempts_max {
                break result.unwrap();
            }

            delay_controller.sleep().await;
        };

        let c_talos_aborts = Arc::clone(&self.talos_aborts_counter);
        let c_agent_errors = Arc::clone(&self.agent_errors_counter);
        let c_db_errors = Arc::clone(&self.db_errors_counter);
        let h_agent_retries = Arc::clone(&self.agent_retries_histogram);

        if agent_errors > 0 || db_errors > 0 || talos_aborts > 0 || attempts > 0 {
            tokio::spawn(async move {
                c_talos_aborts.add(talos_aborts, &[]);
                c_agent_errors.add(agent_errors, &[]);
                c_db_errors.add(db_errors, &[]);
                h_agent_retries.record(attempts as u64, &[]);
            });
        }

        k
    }

    async fn send_to_talos_agent(&self, request: model::CohortCertificationRequest) -> CertificationAttemptOutcome {
        let (snapshot, readvers) = Self::select_snapshot_and_readvers(request.snapshot, request.candidate.readvers);

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
                    conflict: agent_response.conflict.map(|cm| cm.version),
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

    async fn await_for_snapshot<F, Fut>(
        &self,
        get_state_callback_fn: &F,
        previous_conflict: Option<u64>,
        timeout: Duration,
    ) -> Result<CohortCapturedState, SnapshotPollErrorType>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<CohortCapturedState, String>>,
    {
        let conflict = previous_conflict.unwrap_or(0);

        let mut delay_controller = DelayController::new(self.config.backoff_on_conflict.min_ms, self.config.backoff_on_conflict.max_ms);
        let poll_started_at = Instant::now();

        loop {
            let result_local_state = get_state_callback_fn().await;
            match result_local_state {
                Err(reason) => return Err(SnapshotPollErrorType::FetchError { reason }),
                Ok(CohortCapturedState::Proceed(request)) if request.snapshot < conflict => {
                    let waited = poll_started_at.elapsed();
                    if waited >= timeout {
                        return Err(SnapshotPollErrorType::Timeout { waited });
                    }
                    delay_controller.sleep().await;
                    continue;
                }
                Ok(request) => return Ok(request),
            }
        }
    }
}
#[derive(Debug)]
pub enum SnapshotPollErrorType {
    Timeout { waited: Duration },
    FetchError { reason: String },
}
