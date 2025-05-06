use futures::Future;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use talos_common_utils::otel::propagated_context::PropagatedSpanContextData;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use opentelemetry::global;

use tracing::Instrument;

use serde_json::Value;
use time::OffsetDateTime;

use talos_agent::{
    agent::{
        core::{AgentServices, TalosAgentImpl},
        model::{CancelRequestChannelMessage, CertifyRequestChannelMessage},
    },
    api::{AgentConfig, CandidateData, CertificationRequest, TalosAgent, TalosType},
    messaging::{
        api::{Decision, TraceableDecision},
        kafka::KafkaInitializer,
    },
    metrics::{client::MetricsClient, model::Signal},
    mpsc::core::{ReceiverWrapper, SenderWrapper},
};
use talos_rdkafka_utils::kafka_config::KafkaConfig;

use crate::{
    model::ClientErrorKind,
    otel::{
        initialiser::{init_otel_logs_tracing, init_otel_metrics},
        meters::CohortMeters,
    },
};

use crate::{
    delay_controller::DelayController,
    model::{
        self,
        callback::{CertificationCandidateCallbackResponse, OutOfOrderInstallOutcome, OutOfOrderInstallRequest, OutOfOrderInstaller},
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
    otel_meters: CohortMeters,
}

// #[napi]
impl Cohort {
    // #[napi]
    pub async fn create(
        config: Config,
        // Param1: The list of statemap items.
        // Param2: Version to install.
        // Returns error description. If string is empty it means there was no error installing
    ) -> Result<Self, ClientError> {
        let otel_name = format!("{}-cohort_initiator", config.otel_telemetry.name.clone());
        init_otel_logs_tracing(
            otel_name,
            config.otel_telemetry.enable_traces,
            config.otel_telemetry.grpc_endpoint.clone(),
            "info",
        )?;
        if config.otel_telemetry.enable_metrics {
            init_otel_metrics(config.otel_telemetry.grpc_endpoint.clone()).map_err(|otel_error| ClientError {
                kind: ClientErrorKind::Internal,
                reason: format!("Unable to initialise OTEL metrics. Config: ${:?}", config.otel_telemetry),
                cause: Some(otel_error.to_string()),
            })?;
        }

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

        let (tx_decision_ch, rx_decision_ch) = tokio::sync::mpsc::channel::<TraceableDecision>(agent_config.buffer_size as usize);
        let tx_decision = SenderWrapper::<TraceableDecision> { tx: tx_decision_ch };
        let rx_decision = ReceiverWrapper::<TraceableDecision> { rx: rx_decision_ch };

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

        let brokers_list = kafka_config.brokers.join(",");
        let kspan = tracing::info_span!("cohort_init", brokers = %brokers_list);

        let (publisher, consumer) = KafkaInitializer::connect(agent_config.agent.clone(), kafka_config, TalosType::External)
            .instrument(kspan)
            .await
            .map_err(|me| ClientError {
                kind: model::ClientErrorKind::Messaging,
                reason: "Error connecting Talos agent to Kafka.".into(),
                cause: Some(me.reason),
            })?;

        let agent_services = agent.start(rx_certify, rx_cancel, tx_decision, rx_decision, publisher, consumer);

        let metrics_enabled = config.otel_telemetry.enable_metrics;
        Ok(Self {
            config,
            talos_agent: Box::new(agent),
            agent_services,
            otel_meters: CohortMeters::new(metrics_enabled),
        })
    }

    fn select_snapshot_and_readvers(cpt_snapshot: u64, cpt_versions: Vec<u64>) -> (u64, Vec<u64>) {
        if cpt_versions.is_empty() {
            tracing::debug!(
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

        tracing::debug!(
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

    /// Certifies a candidate in talos and install the statemap if the certification is successful.
    /// Uses two callbacks
    ///  - First callback to build the request payload to send to talos for certification
    ///  - The installer callback will be called to do the out of order install.
    // #[tracing::instrument(name = "cohort_certify", skip_all)]
    pub async fn certify<F, Fut, O>(
        &self,
        get_certification_candidate_callback: &F,
        oo_installer: &O,
        trace_parent: Option<String>,
    ) -> Result<model::CertificationResponse, ClientError>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<CertificationCandidateCallbackResponse, String>>,
        O: OutOfOrderInstaller,
    {
        // 1. Get the snapshot
        // 2. Send for certification

        let metric_send_to_talos = Instant::now(); // this will be intentionally overwritten
        let certification_started_at = OffsetDateTime::now_utc().unix_timestamp_nanos(); // this is for metrics which will go into Decision message

        let make_new_span = || tracing::info_span!("certify");

        tracing::debug!("Certifying transaction....");

        let span_certify = if let Some(trace_parent) = trace_parent {
            let propagated_context = PropagatedSpanContextData::new_with_trace_parent(trace_parent);
            let span_context = global::get_text_map_propagator(|propagator| propagator.extract(&propagated_context));
            let new_span = make_new_span();
            new_span.set_parent(span_context);
            new_span
        } else {
            make_new_span()
        };

        let span_certify_id = span_certify.id().clone();
        let main_execution = async move {
            let span_send_to_talos = tracing::info_span!(parent: span_certify_id.clone(), "send_to_talos");
            let response = self
                .send_to_talos(certification_started_at, get_certification_candidate_callback)
                .instrument(span_send_to_talos)
                .await?;
            let metric_send_to_talos = metric_send_to_talos.elapsed().as_nanos() as f64 / 1_000_000_f64;
            self.otel_meters.update_talos_metric(metric_send_to_talos);

            if response.safepoint.is_none() || response.statemaps.is_none() {
                return Ok(response);
            }

            let oooinstall_payload = OutOfOrderInstallRequest {
                xid: response.xid.clone(),
                version: response.version,
                safepoint: response.safepoint.unwrap(),
                statemaps: response.statemaps.clone().unwrap(),
            };

            // 3. OOO install
            let span_install = tracing::info_span!(parent: span_certify_id, "install_ooo");
            self.install_statemaps_oo(oooinstall_payload, oo_installer).instrument(span_install).await?;

            Ok(response)
        };

        let result = main_execution.instrument(span_certify).await;

        tracing::debug!("Certifying transaction ended");

        result
    }

    /// Installs the statemap for candidate messages with committed decisions received from talos.
    async fn install_statemaps_oo<O>(&self, install_payload: OutOfOrderInstallRequest, oo_installer: &O) -> Result<(), ClientError>
    where
        O: OutOfOrderInstaller,
    {
        let mut controller = DelayController::new(self.config.retry_oo_backoff.min_ms, self.config.retry_oo_backoff.max_ms);
        let mut attempt = 0_u32;
        let span_2 = Instant::now();

        let mut is_success = false;
        let mut is_not_save = 0_u64;
        let mut giveups = 0_u64;

        let safepoint = install_payload.safepoint;

        let result = loop {
            attempt += 1;

            let span_3 = Instant::now();

            let tspan = tracing::info_span!("oo installer callback", %attempt);
            let install_result = oo_installer.install(install_payload.clone()).instrument(tspan).await;
            let span_3_val = span_3.elapsed().as_nanos() as f64 / 1_000_000_f64;
            self.otel_meters.update_oo_install_duration(span_3_val);

            let error = match install_result {
                Ok(OutOfOrderInstallOutcome::SafepointCondition) => {
                    is_not_save += 1;
                    // We create this error as "safepoint timeout" in advance. Error is erased if further attempt will be successfull or replaced with another error.
                    Some(ClientError {
                        kind: model::ClientErrorKind::OutOfOrderSnapshotTimeout,
                        reason: format!("Timeout waitig for safepoint: {}", safepoint),
                        cause: None,
                    })
                }
                Ok(_) => {
                    is_success = true;
                    None
                }
                Err(error) => {
                    is_success = false;
                    Some(ClientError {
                        kind: model::ClientErrorKind::OutOfOrderCallbackFailed,
                        reason: error,
                        cause: None,
                    })
                }
            };

            if let Some(client_error) = error {
                if attempt >= self.config.retry_oo_attempts_max {
                    giveups += 1;
                    break Err(client_error);
                }

                // try again
                controller.sleep().await;
            } else {
                break Ok(());
            }
        };

        let span_2_val = span_2.elapsed().as_nanos() as f64 / 1_000_000_f64;
        let total_sleep = controller.total_sleep_time;

        self.otel_meters
            .update_post_oo_install_metrics(is_success, is_not_save, total_sleep, giveups, attempt, span_2_val);

        tracing::debug!("Total attempts used to install: {attempt}");
        result
    }

    async fn send_to_talos<F, Fut>(
        &self,
        certification_started_at: i128,
        get_certification_candidate_callback: &F,
    ) -> Result<model::CertificationResponse, ClientError>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<CertificationCandidateCallbackResponse, String>>,
    {
        let started_at = Instant::now();
        let mut result: Option<Result<CertificationResponse, ClientError>>;

        let mut delay_controller = DelayController::new(self.config.retry_backoff.min_ms, self.config.retry_backoff.max_ms);

        let mut attempts = 0;
        let mut talos_aborts = 0_u64;
        let mut agent_errors = 0_u64;
        let mut db_errors = 0_u64;

        let mut recent_conflict: Option<u64> = None;
        let mut recent_abort: Option<CertificationResponse> = None;

        // The loop exits when either of the below conditions are met.
        //  1. When commit decision is received from talos agent/certifier.
        //  2. When an client abort is requested.
        //  3. When all retries are exhausted.
        let final_result = loop {
            // Await for snapshot and build the certification request payload.
            // Send the certification payload to talos
            match self
                .send_to_talos_attempt(attempts + 1, certification_started_at, &get_certification_candidate_callback, recent_conflict)
                .await
            {
                CertificationAttemptOutcome::Success { mut response } => {
                    response.metadata.duration_ms = started_at.elapsed().as_millis() as u64;
                    response.metadata.attempts = attempts;
                    break Ok(response);
                }
                CertificationAttemptOutcome::Aborted { mut response } => {
                    talos_aborts += 1;
                    response.metadata.duration_ms = started_at.elapsed().as_millis() as u64;
                    response.metadata.attempts = attempts;

                    // TODO: GK - aborts by rule 2 will not have any conflict version.
                    recent_conflict = response.conflict;
                    recent_abort = Some(response.clone());
                    result = Some(Ok(response));
                }
                CertificationAttemptOutcome::AgentError { error } => {
                    result = Some(Err(ClientError::from(error)));
                    agent_errors += 1;
                }

                CertificationAttemptOutcome::Cancelled { reason } => {
                    break Err(ClientError {
                        kind: model::ClientErrorKind::Cancelled,
                        reason,
                        cause: None,
                    });
                }
                CertificationAttemptOutcome::SnapshotTimeout { waited, conflict } => {
                    tracing::error!("Timeout wating for snapshot: {:?}. Waited: {:.2} sec", conflict, waited.as_secs_f32());
                    result = recent_abort.clone().map(Ok);
                }
                CertificationAttemptOutcome::DataError { reason } => {
                    result = Some(Err(ClientError {
                        kind: model::ClientErrorKind::Persistence,
                        reason,
                        cause: None,
                    }));
                    db_errors += 1;
                }
            }

            let rslt_response = result.unwrap();
            attempts += 1;
            if attempts >= self.config.retry_attempts_max {
                break rslt_response;
            }

            match rslt_response {
                Ok(response) => {
                    tracing::debug!(
                        "Unsuccessful transaction: {:?}. Response: {:?} This might retry. Attempts: {}",
                        response.statemaps,
                        response.decision,
                        attempts
                    );
                }
                Err(error) => {
                    tracing::debug!("Unsuccessful transaction with error: {:?}. This might retry. Attempts: {}", error, attempts);
                }
            }

            delay_controller.sleep().await;
        };

        self.otel_meters
            .update_post_send_to_talos_metrics(agent_errors, db_errors, talos_aborts, attempts);

        final_result
    }

    #[tracing::instrument(name = "send_to_talos_attempt", skip(self, get_certification_candidate_callback))]
    async fn send_to_talos_attempt<F, Fut>(
        &self,
        attempt: u32,
        certification_started_at: i128,
        get_certification_candidate_callback: &F,
        previous_conflict: Option<u64>,
    ) -> CertificationAttemptOutcome
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<CertificationCandidateCallbackResponse, String>>,
    {
        let timeout = Duration::from_millis(self.config.snapshot_wait_timeout_ms as u64);

        let request = match self
            .create_candidate_for_certification(get_certification_candidate_callback, previous_conflict, timeout)
            .await
        {
            Err(SnapshotPollErrorType::FetchError { reason }) => return CertificationAttemptOutcome::DataError { reason },
            Err(SnapshotPollErrorType::Timeout { waited }) => {
                return CertificationAttemptOutcome::SnapshotTimeout {
                    waited,
                    conflict: previous_conflict.unwrap(),
                }
            }
            Ok(CertificationCandidateCallbackResponse::Cancelled(reason)) => {
                return CertificationAttemptOutcome::Cancelled { reason };
            }
            Ok(CertificationCandidateCallbackResponse::Proceed(request)) => request,
        };

        tracing::debug!(
            "loaded state: snapshot: {}, candidate: {:?}, headers: {:?}",
            request.snapshot,
            request.candidate,
            request.headers,
        );

        let (snapshot, readvers) = Self::select_snapshot_and_readvers(request.snapshot, request.candidate.readvers);

        let xid = uuid::Uuid::new_v4().to_string();
        let on_commit: Option<Box<Value>> = match request.candidate.on_commit {
            Some(value) => serde_json::to_value(value).ok().map(|x| x.into()),
            None => None,
        };

        let agent_request = CertificationRequest {
            message_key: xid.clone(),
            candidate: CandidateData {
                xid,
                statemap: request.candidate.statemaps.clone(),
                readset: request.candidate.readset,
                writeset: request.candidate.writeset,
                readvers,
                snapshot,
                on_commit,
            },
            timeout: if request.timeout_ms > 0 {
                Some(Duration::from_millis(request.timeout_ms))
            } else {
                None
            },
            headers: request.headers,
            certification_started_at,
            request_created_at: OffsetDateTime::now_utc().unix_timestamp_nanos(),
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
                    statemaps: request.candidate.statemaps,
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

    #[tracing::instrument(name = "create_candidate", skip(self, get_candidate_callback))]
    async fn create_candidate_for_certification<F, Fut>(
        &self,
        get_candidate_callback: &F,
        previous_conflict: Option<u64>,
        timeout: Duration,
    ) -> Result<CertificationCandidateCallbackResponse, SnapshotPollErrorType>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<CertificationCandidateCallbackResponse, String>>,
    {
        let conflict = previous_conflict.unwrap_or(0);

        let mut delay_controller = DelayController::new(self.config.backoff_on_conflict.min_ms, self.config.backoff_on_conflict.max_ms);
        let poll_started_at = Instant::now();

        loop {
            let candidate_callback_result = get_candidate_callback().await;
            match candidate_callback_result {
                Err(reason) => return Err(SnapshotPollErrorType::FetchError { reason }),
                Ok(CertificationCandidateCallbackResponse::Proceed(request)) if request.snapshot < conflict => {
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
