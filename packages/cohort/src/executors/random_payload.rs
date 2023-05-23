use std::{collections::HashMap, sync::Arc};

use time::OffsetDateTime;

use rand::Rng;

use crate::{
    bank_api::BankApi,
    core::{AgentType, Cohort},
    model::{
        bank_account::{as_money, BankAccount},
        requests::{AccountUpdateRequest, BusinessActionType},
    },
    snapshot_api::SnapshotApi,
    state::postgres::database::Database,
};

pub struct RandomPayloadExecutor {}

impl RandomPayloadExecutor {
    pub async fn execute(agent: Arc<AgentType>, database: Arc<Database>, duration_sec: u32) -> Result<(), String> {
        log::info!("Generating test load for {}s", duration_sec);

        let started_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
        loop {
            let cpt_snapshot = SnapshotApi::query(Arc::clone(&database)).await?;
            let accounts = BankApi::get_accounts_as_map(Arc::clone(&database), None).await.unwrap();

            // pick random bank accounts and amount
            let (account1, amount, opt_account2, is_deposit) = Self::pick(&accounts);
            let reloaded_account1 = accounts.get(&account1.number).unwrap();
            let balance = reloaded_account1.balance.clone();

            if let Some(a2) = opt_account2 {
                // two accounts picked, do transfer...

                // Cohort should do local validation before attempting to alter internal state

                let reloaded_account2 = accounts.get(&a2.number).unwrap();
                if balance < as_money(amount.clone(), account1.balance.currency())? {
                    log::warn!("Cannot transfer {:>2} from {} with balance {}", amount, account1.number, balance);
                    continue;
                }

                Cohort::transfer(
                    Arc::clone(&agent),
                    Arc::clone(&database),
                    reloaded_account1,
                    reloaded_account2,
                    amount,
                    cpt_snapshot,
                )
                .await?;
                continue;
            }

            // single account picked, do deposit or withdrawal
            if is_deposit {
                let statemap = vec![HashMap::from([(
                    BusinessActionType::DEPOSIT.to_string(),
                    AccountUpdateRequest::new(account1.number.clone(), amount.clone()).json(),
                )])];
                Cohort::do_bank(
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
                continue;
            }

            // Cohort should do local validation before attempting to alter internal state
            if balance < as_money(amount.clone(), account1.balance.currency())? {
                log::warn!("Cannot withdraw {:>2} from {} with balance {}", amount.clone(), account1.number, balance);
                continue;
            }

            Cohort::do_bank(
                Arc::clone(&agent),
                Arc::clone(&database),
                "W".to_string(),
                reloaded_account1,
                amount.clone(),
                vec![HashMap::from([(
                    BusinessActionType::WITHDRAW.to_string(),
                    AccountUpdateRequest::new(account1.number.clone(), amount.clone()).json(),
                )])],
                cpt_snapshot,
                BankApi::withdraw,
            )
            .await?;

            let elapsed = OffsetDateTime::now_utc().unix_timestamp_nanos() - started_at;
            if (duration_sec as i128 * 1000000000_i128) <= elapsed {
                break;
            }
        }

        let finished_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
        log::info!("The workload completed in {} s", ((finished_at - started_at) as f32) / 1000000000.0_f32);

        let accounts = BankApi::get_accounts(Arc::clone(&database)).await?;
        log::info!("New state of bank accounts is");
        for a in accounts.iter() {
            log::info!("{}", a);
        }

        Ok(())
    }

    fn pick(accounts_map: &HashMap<String, BankAccount>) -> (&BankAccount, String, Option<&BankAccount>, bool) {
        let mut accounts = Vec::<&BankAccount>::new();
        for a in accounts_map.values() {
            accounts.push(a);
        }

        let mut rnd = rand::thread_rng();
        let i = rnd.gen_range(0..accounts.len());
        let account1 = accounts.get(i).unwrap();
        let amount = rnd.gen_range(1..20).to_string();
        if rnd.gen::<bool>() {
            // $coverage:ignore-start
            loop {
                let j = rnd.gen_range(0..accounts.len());
                if i == j {
                    continue;
                }
                let account2 = accounts.get(j).unwrap();
                break (account1, amount, Some(account2), false);
            }
            // $coverage:ignore-end
        } else {
            (account1, amount, Option::<&BankAccount>::None, rnd.gen::<bool>())
        }
    }
}

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use super::*;
    use std::assert_ne;
    use std::collections::HashMap;

    use crate::model::bank_account::BankAccount;
    use crate::model::talos_state::TalosState;

    #[test]
    fn pick() {
        let list = HashMap::from([
            (
                "a1".to_string(),
                BankAccount::aud("a1".to_string(), "a1".to_string(), "1".to_string(), TalosState { version: 1 }),
            ),
            (
                "a2".to_string(),
                BankAccount::aud("a2".to_string(), "a2".to_string(), "2".to_string(), TalosState { version: 2 }),
            ),
            (
                "a3".to_string(),
                BankAccount::aud("a3".to_string(), "a3".to_string(), "3".to_string(), TalosState { version: 3 }),
            ),
        ]);

        let (a1, _, a2, is_deposit) = RandomPayloadExecutor::pick(&list);
        assert!(list.get(&a1.number).is_some());
        if let Some(a2_value) = a2 {
            assert!(list.get(&a2_value.number).is_some());
            assert_ne!(*a1, *a2_value);
            assert!(!is_deposit);
        }
    }
}
// $coverage:ignore-end
