use std::str::FromStr;
use std::{collections::HashMap, sync::Arc};
use time::OffsetDateTime;

use crate::{
    bank_api::BankApi,
    cohort_exec_models::DelayController,
    core::{AgentType, Cohort},
    model::{
        bank_account::as_money,
        requests::{AccountUpdateRequest, BusinessActionType},
    },
    snapshot_api::SnapshotApi,
    state::postgres::database::Database,
};

pub struct SimpleCsvExecutor {}

impl SimpleCsvExecutor {
    pub async fn execute(agent: Arc<AgentType>, database: Arc<Database>, transactions: String) -> Result<(), String> {
        let field_action = 0;
        let field_acc1 = 1;
        let field_amount = 2;
        let field_acc2 = 3;

        let mut reader = csv::Reader::from_reader(transactions.as_bytes());

        let stats_started_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let mut stats_dur_min = u64::MAX;
        let mut stats_dur_max = 0_u64;
        let mut stats_tx_started_at;
        let mut stats_feeder_sleep = 0_i128;
        let mut stats_errors = 0_u64;
        let mut stats_count = 0_u64;
        let mut stats_isolation_conflicts = 0_u64;
        let mut stats_validation_errors = 0_u64;
        let mut stats_retry_count = 0_u64;
        let mut stats_retry_count_min = u64::MAX;
        let mut stats_retry_count_max = 0_u64;

        for (index, rslt_record) in reader.records().enumerate() {
            //log::warn!("\n\n---------------------- {} ----------------------", index);
            if index > 0 && index % 1000 == 0 {
                log::warn!("...processed {}", index);
            }
            let record = rslt_record.map_err(|e| format!("{:?}", e))?;

            let mut retry_count = 0;
            stats_tx_started_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
            stats_count += 1;

            let mut delay_controller = Box::new(DelayController::new(1500));
            loop {
                stats_retry_count += 1;

                retry_count += 1;
                if retry_count > 50 {
                    // Should we give up on it or keep trying?
                    log::warn!("Giving up on failing tx: {} {:?}\n", (index + 1), record.clone());
                    stats_errors += 1;
                    break;
                }

                if retry_count == 1 {
                    log::info!("\n\nexecuting: {}, {:?}", (index + 1), record);
                }

                let cpt_snapshot = SnapshotApi::query(Arc::clone(&database)).await?;
                let action = BusinessActionType::from_str(&record[field_action]).unwrap();
                let all_accounts = BankApi::get_accounts_as_map(Arc::clone(&database), None).await.unwrap();
                let reloaded_account1 = all_accounts.get(&record[field_acc1]).unwrap();
                let balance = reloaded_account1.balance.clone();
                let amount = record[field_amount].to_string();

                let bank_result = match action {
                    BusinessActionType::DEPOSIT => {
                        let statemap = vec![HashMap::from([(
                            BusinessActionType::DEPOSIT.to_string(),
                            AccountUpdateRequest::new(reloaded_account1.number.clone(), amount.clone()).json(),
                        )])];

                        let result = Cohort::do_bank(
                            Arc::clone(&agent),
                            Arc::clone(&database),
                            "D".to_string(),
                            reloaded_account1,
                            amount,
                            statemap,
                            cpt_snapshot,
                            BankApi::deposit,
                        )
                        .await?;

                        Some(result)
                    }
                    BusinessActionType::WITHDRAW => {
                        if balance < as_money(amount.clone(), balance.currency())? {
                            log::warn!("Cannot withdraw {:>2} from {} with balance {}", amount, reloaded_account1.number, balance);
                            None
                        } else {
                            let statemap = vec![HashMap::from([(
                                BusinessActionType::WITHDRAW.to_string(),
                                AccountUpdateRequest::new(reloaded_account1.number.clone(), amount.clone()).json(),
                            )])];

                            let result = Cohort::do_bank(
                                Arc::clone(&agent),
                                Arc::clone(&database),
                                "W".to_string(),
                                reloaded_account1,
                                amount,
                                statemap,
                                cpt_snapshot,
                                BankApi::withdraw,
                            )
                            .await?;

                            Some(result)
                        }
                    }
                    BusinessActionType::TRANSFER => {
                        let reloaded_account2 = all_accounts.get(&record[field_acc2]).unwrap();
                        if balance < as_money(amount.clone(), balance.currency())? {
                            log::warn!("Cannot transfer {:>2} from {} with balance {}", amount, reloaded_account1.number, balance);
                            None
                        } else {
                            let result = Cohort::transfer(
                                Arc::clone(&agent),
                                Arc::clone(&database),
                                reloaded_account1,
                                reloaded_account2,
                                amount,
                                cpt_snapshot,
                            )
                            .await?;
                            Some(result)
                        }
                    }
                };

                let was_error = match bank_result {
                    None => {
                        stats_validation_errors += 1;
                        true
                    }

                    Some(result) => {
                        if result.is_tx_isolation_error {
                            stats_isolation_conflicts += 1;
                            true
                        } else {
                            result.is_aborted
                        }
                    }
                };

                if was_error {
                    let st = OffsetDateTime::now_utc().unix_timestamp_nanos();
                    log::debug!("{} (attempt: {}). Waiting before we try again: {:?}", (index + 1), retry_count, record);
                    delay_controller.sleep().await;

                    let sleep_time = OffsetDateTime::now_utc().unix_timestamp_nanos() - st;
                    stats_feeder_sleep += sleep_time;
                    log::debug!("{} (attempt: {}). Wited for {}ms on {:?}", (index + 1), retry_count, sleep_time, record);
                } else {
                    break;
                }
            }

            let dur = (OffsetDateTime::now_utc().unix_timestamp_nanos() - stats_tx_started_at) as u64;
            if dur > stats_dur_max {
                stats_dur_max = dur;
            }
            if dur < stats_dur_min {
                stats_dur_min = dur;
            }
            if retry_count < stats_retry_count_min {
                stats_retry_count_min = retry_count;
            }
            if retry_count > stats_retry_count_max {
                stats_retry_count_max = retry_count;
            }
        }

        let stats_finished_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let stats_duration = ((stats_finished_at - stats_started_at) as f32) / 1000000000.0_f32;
        log::warn!("The workload completed");

        let accounts = BankApi::get_accounts(Arc::clone(&database)).await?;
        log::warn!("New state of bank accounts is");
        for a in accounts.iter() {
            log::warn!("{}", a);
        }
        let stats = format!(
            r#"
Duration total   (sec): {}
Duration working (sec): {}
Duration min     (sec): {}
Duration max     (sec): {}
Completed count       : {}
Rate             (TPS): {}
Retry count total     : {}
Retry count min       : {}
Retry count max       : {}
Retry avg   (attempts): {}
Isolation conflicts   : {}
Valdiation errors     : {}
Errors count          : {}"#,
            stats_duration,
            stats_duration - (stats_feeder_sleep as f32 / 1000000000.0_f32),
            stats_dur_min as f32 / 1000000000.0_f32,
            stats_dur_max as f32 / 1000000000.0_f32,
            stats_count,
            stats_count as f32 / stats_duration,
            stats_retry_count,
            stats_retry_count_min,
            stats_retry_count_max,
            (stats_retry_count as f32 / stats_count as f32) - 1.0,
            stats_isolation_conflicts,
            stats_validation_errors,
            stats_errors,
        );
        log::warn!("{}", stats);

        Ok(())
    }
}
