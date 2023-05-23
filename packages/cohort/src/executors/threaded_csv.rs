use std::sync::Arc;

use csv::StringRecord;
use std::str::FromStr;
use time::OffsetDateTime;
use tokio::task::JoinHandle;

use crate::{
    bank_api::BankApi,
    cohort_exec_models::{DelayController, TxExecutionOutcome, TxExecutionResult, TxNonFatalError},
    cohort_stats::Stats,
    core::{AgentType, Cohort},
    model::{bank_account::as_money, requests::BusinessActionType},
    snapshot_api::SnapshotApi,
    state::postgres::database::Database,
};

pub struct ThreadedCsvExecutor {}

impl ThreadedCsvExecutor {
    pub async fn start(agent: Arc<AgentType>, database: Arc<Database>, transactions: String, threads: u8) -> Result<(), String> {
        let mut tasks = Vec::<JoinHandle<Result<Stats, String>>>::new();
        let started_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
        for _ in 1..=threads {
            let agent_ref = Arc::clone(&agent);
            let db = Arc::clone(&database);
            let csv = transactions.clone();
            let h = tokio::spawn(async move { Self::execute_all(agent_ref, db, csv).await });
            tasks.push(h);
        }

        let mut stats = Stats::new();
        for th in tasks {
            if let Ok(task_results) = th.await {
                if let Ok(task_stats) = task_results {
                    stats.merge(task_stats);
                } else {
                    stats.exceptions += 1;
                }
            } else {
                stats.threading_errors += 1;
            }
        }
        let finished_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
        log::warn!("The processing has finished, computing stats....");
        log::warn!("{}", stats.generate_report(threads, (finished_at - started_at) as f32));

        Ok(())
    }

    pub async fn execute_all(agent: Arc<AgentType>, database: Arc<Database>, transactions: String) -> Result<Stats, String> {
        let mut reader = csv::Reader::from_reader(transactions.as_bytes());

        let mut stats = Stats::new();

        let started_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
        for (_, rslt_record) in reader.records().enumerate() {
            let agent_ref = Arc::clone(&agent);
            let db = Arc::clone(&database);
            let record = rslt_record.map_err(|e| format!("{:?}", e))?;
            let result = Self::execute_until_giveup(agent_ref, db, record).await?;
            stats.total_count += 1;

            match result {
                TxExecutionOutcome::GaveUp { stats: item_stats } => {
                    stats.giveup_count += 1;
                    stats.merge(item_stats);
                }
                TxExecutionOutcome::Executed { stats: item_stats } => {
                    stats.merge(item_stats);
                }
            }
        }
        let finished_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
        stats.on_set_completed(finished_at - started_at);

        Ok(stats)
    }

    pub async fn execute_until_giveup(agent: Arc<AgentType>, database: Arc<Database>, transaction: StringRecord) -> Result<TxExecutionOutcome, String> {
        let mut delay_controller = Box::new(DelayController::new(1500));

        let mut stats = Stats::new();
        let started_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
        loop {
            let agent_ref = Arc::clone(&agent);
            let db = Arc::clone(&database);
            let tx = transaction.clone();

            let result = Self::execute_once(agent_ref, db, tx).await?;
            let finished_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
            stats.on_tx_completed(finished_at - started_at);

            match result {
                TxExecutionResult::RetriableError { reason } => {
                    match reason {
                        TxNonFatalError::ErrorIsolation => stats.isolation_errors += 1,
                        TxNonFatalError::ErrorValidation => stats.validation_errors += 1,
                        TxNonFatalError::ErrorAborted => stats.aborts += 1,
                    }
                    if stats.retry_count == 51 {
                        return Ok(TxExecutionOutcome::GaveUp { stats });
                    }
                    stats.retry_count += 1;
                    delay_controller.sleep().await;
                }

                TxExecutionResult::Success => {
                    stats.sleep_time = delay_controller.total_sleep_time;
                    return Ok(TxExecutionOutcome::Executed { stats });
                }
            }
        }
    }

    pub async fn execute_once(agent: Arc<AgentType>, database: Arc<Database>, transaction: StringRecord) -> Result<TxExecutionResult, String> {
        let field_action = 0;
        let field_acc1 = 1;
        let field_amount = 2;
        let field_acc2 = 3;

        let action = BusinessActionType::from_str(&transaction[field_action]).unwrap();
        if action != BusinessActionType::TRANSFER {
            return Err("Unsupported bank operation".into());
        }

        let cpt_snapshot = SnapshotApi::query(Arc::clone(&database)).await?;

        let a1 = transaction[field_acc1].to_string();
        let a2 = transaction[field_acc2].to_string();

        let all_accounts = BankApi::get_accounts_as_map(Arc::clone(&database), Some(vec![a1.clone(), a2.clone()]))
            .await
            .unwrap();
        let reloaded_account1 = all_accounts.get(&a1).unwrap();
        let balance = reloaded_account1.balance.clone();
        let reloaded_account2 = all_accounts.get(&a2).unwrap();
        let amount = transaction[field_amount].to_string();

        if balance < as_money(amount.clone(), balance.currency())? {
            log::warn!("Cannot transfer {:>2} from {} with balance {}", amount, reloaded_account1.number, balance);
            return Ok(TxExecutionResult::RetriableError {
                reason: TxNonFatalError::ErrorValidation,
            });
        }

        let bank_result = Cohort::transfer(
            Arc::clone(&agent),
            Arc::clone(&database),
            reloaded_account1,
            reloaded_account2,
            amount,
            cpt_snapshot,
        )
        .await?;
        if bank_result.is_aborted {
            Ok(TxExecutionResult::RetriableError {
                reason: TxNonFatalError::ErrorAborted,
            })
        } else {
            Ok(TxExecutionResult::Success)
        }
    }
}
