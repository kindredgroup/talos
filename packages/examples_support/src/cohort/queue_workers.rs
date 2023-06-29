use std::{sync::Arc, time::Duration};

use cohort::{
    bank_api::BankApi,
    config_loader::ConfigLoader,
    core::{AgentType, Cohort},
    delay_controller::DelayController,
    metrics::{Span, Stats, TxExecSpans},
    model::{
        bank_account::as_money,
        requests::TransferRequest,
        tx_execution::{TxExecutionOutcome, TxExecutionResult, TxNonFatalError},
    },
    snapshot_api::SnapshotApi,
    state::postgres::database::Database,
};
use metrics::model::{MicroMetrics, MinMax};
use time::OffsetDateTime;
use tokio::task::JoinHandle;

pub struct QueueProcessor {}
impl QueueProcessor {
    pub async fn process(
        queue: Arc<async_channel::Receiver<TransferRequest>>,
        tx_metrics: Arc<async_channel::Sender<Stats>>,
        threads: u64,
        max_retry: u64,
        database: Arc<Database>,
    ) -> Result<(), String> {
        let cfg_agent = ConfigLoader::load_agent_config()?;
        let cfg_kafka = ConfigLoader::load_kafka_config()?;

        let agent = Arc::new(Cohort::init_agent(cfg_agent, cfg_kafka).await);
        let agent_ref = Arc::clone(&agent);
        let mut tasks = Vec::<JoinHandle<Result<(), String>>>::new();
        let started_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
        for thread_number in 1..=threads {
            let task_h = Self::launch_handler(
                thread_number,
                max_retry,
                Arc::clone(&queue),
                Arc::clone(&tx_metrics),
                Arc::clone(&agent_ref),
                Arc::clone(&database),
            )
            .await;
            tasks.push(task_h);
        }

        // Collect agent stats and produce metrics
        let mut i = 1;
        let mut errors_count = 0;
        for task in tasks {
            if let Err(e) = task.await.unwrap() {
                errors_count += 1;
                log::error!("{:?}", e);
            }
            if i % 10 == 0 {
                log::warn!("Initiator thread {} of {} finished.", i, threads);
            }

            i += 1;
        }

        let finished_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
        if let Some(report) = agent.collect_metrics().await {
            let duration_ms = Duration::from_nanos((finished_at - started_at) as u64).as_millis() as u64;
            report.print(duration_ms, errors_count);
        }

        Ok(())
    }

    async fn launch_handler(
        thread_number: u64,
        max_retry: u64,
        queue: Arc<async_channel::Receiver<TransferRequest>>,
        tx_metrics: Arc<async_channel::Sender<Stats>>,
        agent: Arc<AgentType>,
        database: Arc<Database>,
    ) -> JoinHandle<Result<(), String>> {
        let queue_ref = Arc::clone(&queue);
        tokio::spawn(async move {
            let mut metrics = MicroMetrics::new(1_000_000_000_f32, true);

            let mut span1_get_accounts = MinMax::default();
            let mut span2_get_snap_ver = MinMax::default();
            let mut span3_certify = MinMax::default();
            let mut span4_wait_for_safepoint = MinMax::default();
            let mut span5_install = MinMax::default();

            let mut total_attempts = 0;
            loop {
                let tx_metrics = Arc::clone(&tx_metrics);
                match queue_ref.recv().await {
                    Err(_) => break,
                    Ok(tx_req) => {
                        metrics.clock_start();
                        let rslt_outcome = Self::execute_until_giveup(thread_number, max_retry, tx_req, Arc::clone(&agent), Arc::clone(&database)).await;
                        let elapsed = metrics.clock_end();

                        if let Err(e) = rslt_outcome {
                            log::warn!(
                                "Thread {} cannot process more requests. Processed: {}, Error executing tx: {}",
                                thread_number,
                                metrics.count,
                                e
                            );
                            break;
                        }

                        let outcome = rslt_outcome.unwrap();

                        let (mut stats, is_giveup) = match outcome {
                            TxExecutionOutcome::GaveUp { stats } => (stats, true),
                            TxExecutionOutcome::Executed { stats } => (stats, false),
                        };

                        if is_giveup {
                            stats.giveup_count += 1;
                        }

                        span1_get_accounts.merge(stats.getaccounts.clone());
                        span2_get_snap_ver.merge(stats.getsnap.clone());
                        span3_certify.merge(stats.certify.clone());
                        span4_wait_for_safepoint.merge(stats.waiting.clone());
                        span5_install.merge(stats.installing.clone());

                        total_attempts += stats.total_count;

                        if elapsed >= 1_000_000_000 && thread_number == 1 {
                            metrics.sample_end();
                            log::warn!(
                                "METRIC (cohort) :{:>3},{},{},{},{},{},{},{},{},{},{},{},'-',{},{},{},{},{}",
                                thread_number,
                                total_attempts,
                                Duration::from_nanos(span1_get_accounts.min as u64).as_micros(),
                                Duration::from_nanos(span1_get_accounts.max as u64).as_micros(),
                                Duration::from_nanos(span2_get_snap_ver.min as u64).as_micros(),
                                Duration::from_nanos(span2_get_snap_ver.max as u64).as_micros(),
                                Duration::from_nanos(span3_certify.min as u64).as_micros(),
                                Duration::from_nanos(span3_certify.max as u64).as_micros(),
                                Duration::from_nanos(span4_wait_for_safepoint.min as u64).as_micros(),
                                Duration::from_nanos(span4_wait_for_safepoint.max as u64).as_micros(),
                                Duration::from_nanos(span5_install.min as u64).as_micros(),
                                Duration::from_nanos(span5_install.max as u64).as_micros(),
                                Duration::from_nanos(span1_get_accounts.sum as u64).as_micros(),
                                Duration::from_nanos(span2_get_snap_ver.sum as u64).as_micros(),
                                Duration::from_nanos(span3_certify.sum as u64).as_micros(),
                                Duration::from_nanos(span4_wait_for_safepoint.sum as u64).as_micros(),
                                Duration::from_nanos(span5_install.sum as u64).as_micros(),
                            );

                            span1_get_accounts.reset();
                            span2_get_snap_ver.reset();
                            span3_certify.reset();
                            span4_wait_for_safepoint.reset();
                            span5_install.reset();
                            total_attempts = 0;
                        }

                        if metrics.count % 1000.0 == 0.0 && thread_number == 1 {
                            log::info!("Thread {:>2} processed: {}", thread_number, metrics.count);
                        }
                        tokio::spawn(async move { tx_metrics.send(stats).await });
                    }
                }
            }

            log::debug!("Thread {:>2} stopped.", thread_number);
            Ok(())
        })
    }

    pub async fn execute_until_giveup(
        thread_number: u64,
        max_retry: u64,
        tx_request: TransferRequest,
        agent: Arc<AgentType>,
        database: Arc<Database>,
    ) -> Result<TxExecutionOutcome, String> {
        let mut delay_controller = Box::new(DelayController::new(1500));
        let mut stats = Stats::new();

        loop {
            let started_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
            let result = Self::execute_once(thread_number, &tx_request, Arc::clone(&agent), Arc::clone(&database)).await?;
            let finished_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
            stats.total_count += 1;
            stats.on_tx_finished(started_at, finished_at);
            match result {
                TxExecutionResult::RetriableError { reason, metrics } => {
                    stats.getaccounts.add(metrics.span1_get_accounts.duration());
                    stats.getsnap.add(metrics.span2_get_snap_ver.duration());
                    stats.certify.add(metrics.span3_certify.duration());
                    stats.waiting.add(metrics.span4_wait_for_safepoint.duration());
                    stats.installing.add(metrics.span5_install.duration());

                    match reason {
                        TxNonFatalError::ErrorIsolation => stats.isolation_errors += 1,
                        TxNonFatalError::ErrorValidation => stats.validation_errors += 1,
                        TxNonFatalError::ErrorAborted => stats.aborts += 1,
                    }
                    if stats.retry_count >= max_retry {
                        break Ok(TxExecutionOutcome::GaveUp { stats });
                    }
                    delay_controller.sleep().await;
                    stats.sleep_time = delay_controller.total_sleep_time;
                    stats.inc_retry_count();
                }

                TxExecutionResult::Success { metrics } => {
                    stats.on_tx_finished(started_at, finished_at);
                    stats.getaccounts.add(metrics.span1_get_accounts.duration());
                    stats.getsnap.add(metrics.span2_get_snap_ver.duration());
                    stats.certify.add(metrics.span3_certify.duration());
                    stats.waiting.add(metrics.span4_wait_for_safepoint.duration());
                    stats.installing.add(metrics.span5_install.duration());
                    break Ok(TxExecutionOutcome::Executed { stats });
                }
            }
        }
    }

    pub async fn execute_once(
        thread_number: u64,
        tx_request: &TransferRequest,
        agent: Arc<AgentType>,
        database: Arc<Database>,
    ) -> Result<TxExecutionResult, String> {
        // span 1 (get_accounts_as_map)
        let s1_getacc_s = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let all_accounts = BankApi::get_accounts_as_map(Arc::clone(&database), tx_request.from.clone(), tx_request.to.clone()).await?;
        let s1_getacc_f = OffsetDateTime::now_utc().unix_timestamp_nanos();

        let account_from = all_accounts.get(&tx_request.from).unwrap();
        let balance = account_from.balance.clone();
        let account_to = all_accounts.get(&tx_request.to).unwrap();

        if balance < as_money(tx_request.amount.clone(), balance.currency())? {
            log::warn!(
                "Thread: {} Cannot transfer {:>2} from {} with balance {}",
                thread_number,
                tx_request.amount,
                account_from.number,
                balance
            );
            return Ok(TxExecutionResult::RetriableError {
                reason: TxNonFatalError::ErrorValidation,
                metrics: TxExecSpans::default(),
            });
        }

        // span 2 (SnapshotApi::query)
        let s2_getsnap_s = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let cpt_snapshot = SnapshotApi::query(Arc::clone(&database)).await?;
        let s2_getsnap_f = OffsetDateTime::now_utc().unix_timestamp_nanos();

        let mut bank_result = Cohort::transfer(
            Arc::clone(&agent),
            Arc::clone(&database),
            account_from,
            account_to,
            tx_request.amount.clone(),
            cpt_snapshot,
        )
        .await?;

        bank_result.metrics.span1_get_accounts = Span::new(s1_getacc_s, s1_getacc_f);
        bank_result.metrics.span2_get_snap_ver = Span::new(s2_getsnap_s, s2_getsnap_f);

        if bank_result.is_aborted {
            Ok(TxExecutionResult::RetriableError {
                reason: TxNonFatalError::ErrorAborted,
                metrics: bank_result.metrics,
            })
        } else if bank_result.is_tx_isolation_error {
            Ok(TxExecutionResult::RetriableError {
                reason: TxNonFatalError::ErrorIsolation,
                metrics: bank_result.metrics,
            })
        } else {
            Ok(TxExecutionResult::Success { metrics: bank_result.metrics })
        }
    }
}
