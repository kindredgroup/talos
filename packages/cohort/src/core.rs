// $coverage:ignore-start
use std::collections::HashMap;
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use time::OffsetDateTime;

use talos_agent::agent::core::TalosAgentImpl;
use talos_agent::agent::model::{CancelRequestChannelMessage, CertifyRequestChannelMessage};
use talos_agent::api::{AgentConfig, CandidateData, CertificationRequest, CertificationResponse, KafkaConfig, StateMap, TalosAgent};
use talos_agent::messaging::api::{Decision, DecisionMessage};
use talos_agent::messaging::kafka::KafkaInitializer;
use talos_agent::metrics::client::MetricsClient;
use talos_agent::metrics::core::Metrics;
use talos_agent::metrics::model::Signal;
use talos_agent::mpsc::core::{ReceiverWrapper, SenderWrapper};

use crate::bank_api::BankApi;

use crate::model::bank_account::{as_money, BankAccount};
use crate::model::requests::{AccountUpdateRequest, BusinessActionType, TransferRequest};

use crate::model::snapshot::Snapshot;
use crate::replicator::core::StatemapItem;
use crate::snapshot_api::SnapshotApi;

use crate::state::postgres::data_access::PostgresApi;
use crate::state::postgres::database::Database;

use crate::tx_batch_executor::BatchExecutor;

pub struct Cohort {
    agent: Box<dyn TalosAgent + Sync + Send>,
    database: Arc<Database>,
}

impl Cohort {
    pub async fn init_agent(config: AgentConfig, kafka_config: KafkaConfig) -> Box<dyn TalosAgent + Sync + Send> {
        let (tx_certify_ch, rx_certify_ch) = tokio::sync::mpsc::channel::<CertifyRequestChannelMessage>(config.buffer_size);
        let tx_certify = SenderWrapper::<CertifyRequestChannelMessage> { tx: tx_certify_ch };
        let rx_certify = ReceiverWrapper::<CertifyRequestChannelMessage> { rx: rx_certify_ch };

        let (tx_decision_ch, rx_decision_ch) = tokio::sync::mpsc::channel::<DecisionMessage>(config.buffer_size);
        let tx_decision = SenderWrapper::<DecisionMessage> { tx: tx_decision_ch };
        let rx_decision = ReceiverWrapper::<DecisionMessage> { rx: rx_decision_ch };

        let (tx_cancel_ch, rx_cancel_ch) = tokio::sync::mpsc::channel::<CancelRequestChannelMessage>(config.buffer_size);
        let tx_cancel = SenderWrapper::<CancelRequestChannelMessage> { tx: tx_cancel_ch };
        let rx_cancel = ReceiverWrapper::<CancelRequestChannelMessage> { rx: rx_cancel_ch };

        let (publisher, consumer) = KafkaInitializer::connect(config.agent.clone(), kafka_config)
            .await
            .expect("Cannot connect to kafka...");

        let metrics: Option<Metrics> = None;
        let metrics_client: Option<Box<MetricsClient<SenderWrapper<Signal>>>> = None;

        let agent = TalosAgentImpl::new(
            config.clone(),
            Arc::new(Box::new(tx_certify)),
            tx_cancel,
            metrics,
            Arc::new(metrics_client),
            || {
                let (tx_ch, rx_ch) = tokio::sync::mpsc::channel::<CertificationResponse>(1);
                (SenderWrapper { tx: tx_ch }, ReceiverWrapper { rx: rx_ch })
            },
        );

        agent
            .start(rx_certify, rx_cancel, tx_decision, rx_decision, publisher, consumer)
            .expect("unable to start agent");

        Box::new(agent)
    }

    pub fn new(agent: Box<dyn TalosAgent + Sync + Send>, database: Arc<Database>) -> Self {
        Cohort { agent, database }
    }

    pub async fn execute_batch_workload(&self) -> Result<(), String> {
        let accounts = BankApi::get_accounts(Arc::clone(&self.database)).await?;
        let account1 = accounts.iter().find(|a| a.number == "00001").unwrap();
        let account2 = accounts.iter().find(|a| a.number == "00002").unwrap();

        let version = account1.talos_state.version + 1;

        let batch: Vec<StatemapItem> = vec![
            StatemapItem::new(
                BusinessActionType::WITHDRAW.to_string(),
                version,
                AccountUpdateRequest::new("00001".to_string(), account1.balance.amount().to_string()).json(),
            ),
            StatemapItem::new(
                BusinessActionType::WITHDRAW.to_string(),
                version,
                AccountUpdateRequest::new("00002".to_string(), account2.balance.amount().to_string()).json(),
            ),
            StatemapItem::new(
                BusinessActionType::DEPOSIT.to_string(),
                version,
                AccountUpdateRequest::new("00001".to_string(), "50".to_string()).json(),
            ),
            StatemapItem::new(
                BusinessActionType::DEPOSIT.to_string(),
                version,
                AccountUpdateRequest::new("00002".to_string(), "50".to_string()).json(),
            ),
            StatemapItem::new(
                BusinessActionType::TRANSFER.to_string(),
                version,
                TransferRequest::new("00001".to_string(), "00002".to_string(), "10.51".to_string()).json(),
            ),
            StatemapItem::new(
                BusinessActionType::TRANSFER.to_string(),
                version,
                TransferRequest::new("00001".to_string(), "00002".to_string(), "0.49".to_string()).json(),
            ),
            StatemapItem::new(
                BusinessActionType::TRANSFER.to_string(),
                version,
                TransferRequest::new("00002".to_string(), "00001".to_string(), "1.01".to_string()).json(),
            ),
        ];

        let mut manual_tx_api = PostgresApi {
            client: self.database.get().await,
        };

        match BatchExecutor::execute(&mut manual_tx_api, batch, Some(version)).await {
            Ok(affected_rows) => {
                log::info!("Successfully completed batch of transactions! Updated: {} rows", affected_rows);
                let accounts = BankApi::get_accounts(Arc::clone(&self.database)).await?;
                log::info!("New state of bank accounts is");
                for a in accounts.iter() {
                    log::info!("{}", a);
                }

                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub async fn execute_workload(&self, transactions: String) -> Result<(), String> {
        let field_action = 0;
        let field_acc1 = 1;
        let field_amount = 2;
        let field_acc2 = 3;

        let started_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let mut reader = csv::Reader::from_reader(transactions.as_bytes());

        for (index, rslt_record) in reader.records().enumerate() {
            let record = rslt_record.map_err(|e| format!("{:?}", e))?;

            let mut retry_count = 0;

            loop {
                retry_count += 1;
                if retry_count > 10 {
                    // Should we give up on it or keep trying?
                    log::warn!("Giving up on failing tx: {} {:?}\n", (index + 1), record.clone());
                    break;
                }

                if retry_count == 1 {
                    log::info!("\n\nexecuting: {}, {:?}", (index + 1), record);
                } else {
                    log::info!("retrying: {} (attempt: {}), {:?}", (index + 1), retry_count, record);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }

                let cpt_snapshot = SnapshotApi::query(Arc::clone(&self.database)).await?;
                let action = BusinessActionType::from_str(&record[field_action]).unwrap();
                let all_accounts = BankApi::get_accounts_as_map(Arc::clone(&self.database)).await.unwrap();
                let reloaded_account1 = all_accounts.get(&record[field_acc1]).unwrap();
                let balance = reloaded_account1.balance.clone();
                let amount = record[field_amount].to_string();

                let is_commit = match action {
                    BusinessActionType::DEPOSIT => {
                        let statemap = vec![HashMap::from([(
                            BusinessActionType::DEPOSIT.to_string(),
                            AccountUpdateRequest::new(reloaded_account1.number.clone(), amount.clone()).json(),
                        )])];
                        self.do_bank("D".to_string(), reloaded_account1, amount, statemap, cpt_snapshot, BankApi::deposit)
                            .await?
                    }
                    BusinessActionType::WITHDRAW => {
                        if balance < as_money(amount.clone(), balance.currency())? {
                            log::warn!("Cannot withdraw {:>2} from {} with balance {}", amount, reloaded_account1.number, balance);
                            true
                        } else {
                            let statemap = vec![HashMap::from([(
                                BusinessActionType::WITHDRAW.to_string(),
                                AccountUpdateRequest::new(reloaded_account1.number.clone(), amount.clone()).json(),
                            )])];
                            self.do_bank("W".to_string(), reloaded_account1, amount, statemap, cpt_snapshot, BankApi::withdraw)
                                .await?
                        }
                    }
                    BusinessActionType::TRANSFER => {
                        let reloaded_account2 = all_accounts.get(&record[field_acc2]).unwrap();
                        if balance < as_money(amount.clone(), balance.currency())? {
                            log::warn!("Cannot transfer {:>2} from {} with balance {}", amount, reloaded_account1.number, balance);
                            true
                        } else {
                            self.transfer(reloaded_account1, reloaded_account2, amount, cpt_snapshot).await?
                        }
                    }
                };

                if is_commit {
                    break;
                }
            }
        }

        let finished_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
        log::info!("The workload completed in {} s", ((finished_at - started_at) as f32) / 1000000000.0_f32);

        let accounts = BankApi::get_accounts(Arc::clone(&self.database)).await?;
        log::info!("New state of bank accounts is");
        for a in accounts.iter() {
            log::info!("{}", a);
        }

        Ok(())
    }

    pub async fn generate_workload(&self, duration_sec: u32) -> Result<(), String> {
        log::info!("Generating test load for {}s", duration_sec);

        let started_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
        loop {
            let cpt_snapshot = SnapshotApi::query(Arc::clone(&self.database)).await?;
            let accounts = BankApi::get_accounts_as_map(Arc::clone(&self.database)).await.unwrap();

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

                self.transfer(reloaded_account1, reloaded_account2, amount, cpt_snapshot).await?;
                continue;
            }

            // single account picked, do deposit or withdrawal
            if is_deposit {
                let statemap = vec![HashMap::from([(
                    BusinessActionType::DEPOSIT.to_string(),
                    AccountUpdateRequest::new(account1.number.clone(), amount.clone()).json(),
                )])];
                self.do_bank("D".to_string(), reloaded_account1, amount, statemap, cpt_snapshot, BankApi::deposit)
                    .await?;
                continue;
            }

            // Cohort should do local validation before attempting to alter internal state
            if balance < as_money(amount.clone(), account1.balance.currency())? {
                log::warn!("Cannot withdraw {:>2} from {} with balance {}", amount.clone(), account1.number, balance);
                continue;
            }

            self.do_bank(
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
            //tokio::time::sleep(Duration::from_millis(1000)).await;
        }

        let finished_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
        log::info!("The workload completed in {} s", ((finished_at - started_at) as f32) / 1000000000.0_f32);

        let accounts = BankApi::get_accounts(Arc::clone(&self.database)).await?;
        log::info!("New state of bank accounts is");
        for a in accounts.iter() {
            log::info!("{}", a);
        }

        Ok(())
    }

    async fn do_bank<F, R>(
        &self,
        action: String,
        account: &BankAccount,
        amount: String,
        statemap: StateMap,
        cpt_snapshot: Snapshot,
        op_impl: F,
    ) -> Result<bool, String>
    where
        F: Fn(Arc<Database>, AccountUpdateRequest, u64) -> R,
        R: Future<Output = Result<u64, String>>,
    {
        let (shanpshot_version, read_vers) = Self::select_snapshot_and_readvers(cpt_snapshot.version, vec![account.talos_state.version]);

        let xid = uuid::Uuid::new_v4().to_string();
        let cert_req = CertificationRequest {
            message_key: "cohort-sample".to_string(),
            candidate: CandidateData {
                xid: xid.clone(),
                readset: vec![account.number.to_string()],
                readvers: read_vers,
                snapshot: shanpshot_version,
                writeset: vec![account.number.to_string()],
                statemap: Some(statemap),
            },
            timeout: Some(Duration::from_secs(10)),
        };

        let rslt_cert = self.agent.certify(cert_req).await;
        if let Err(e) = rslt_cert {
            log::warn!(
                "Error communicating via agent: {}, xid: {}, operation: '{}' {} {}",
                e.reason,
                xid,
                action,
                amount,
                account,
            );
            return Err(format!("{:?}", e));
        }

        let resp = rslt_cert.unwrap();
        if Decision::Aborted == resp.decision {
            log::debug!("Aborted by talos: xid: {}, operation: '{}' {} {}", xid, action, amount, account);
            return Ok(false);
        }

        // Talos gave "go ahead"
        log::debug!("Running: '{}' {} {}", action, account.number.clone(), amount.clone());

        // Check safepoint condition before installing ...

        let safepoint = resp.safepoint.unwrap(); // this is safe for 'Committed'

        log::debug!(
            "... waiting for safepoint: {} on '{}' {} {}",
            safepoint,
            action,
            account.number.clone(),
            amount.clone()
        );
        SnapshotApi::await_until_safe(Arc::clone(&self.database), safepoint).await?;

        // install
        log::debug!("... installing '{}' {} {}", action, account.number.clone(), amount.clone());
        let rslt = op_impl(
            Arc::clone(&self.database),
            AccountUpdateRequest::new(account.number.clone(), amount.clone()),
            resp.version,
        )
        .await;

        if rslt.is_err() {
            let e = rslt.unwrap_err();
            if e.contains("could not serialize access due to concurrent update") {
                log::debug!(
                    "tx conflict when running '{}' {} {}. Err: {}, Moving on...\n",
                    action,
                    account.number.clone(),
                    amount,
                    e
                );
                Ok(true)
            } else {
                Err(e)
            }
        } else {
            log::debug!(
                "updated {} rows when running '{}' {} {}\n",
                rslt.unwrap(),
                action,
                account.number.clone(),
                amount
            );
            Ok(true)
        }
    }

    async fn transfer(&self, from: &BankAccount, to: &BankAccount, amount: String, cpt_snapshot: Snapshot) -> Result<bool, String> {
        let (shanpshot_version, read_vers) = Self::select_snapshot_and_readvers(cpt_snapshot.version, vec![from.talos_state.version, to.talos_state.version]);

        let xid = uuid::Uuid::new_v4().to_string();
        let statemap = vec![HashMap::from([(
            BusinessActionType::TRANSFER.to_string(),
            TransferRequest::new(from.number.clone(), to.number.clone(), amount.clone()).json(),
        )])];
        let cert_req = CertificationRequest {
            message_key: "cohort-sample".to_string(),
            candidate: CandidateData {
                xid: xid.clone(),
                readset: vec![from.number.to_string(), to.number.to_string()],
                readvers: read_vers,
                snapshot: shanpshot_version,
                writeset: vec![from.number.to_string(), to.number.to_string()],
                statemap: Some(statemap),
            },
            timeout: Some(Duration::from_secs(10)),
        };

        let rslt_cert = self.agent.certify(cert_req).await;
        if let Err(e) = rslt_cert {
            log::warn!(
                "Error communicating via agent: {}, xid: {}, operation: 'transfer' {} from {} to {}",
                e.reason,
                xid,
                amount,
                from,
                to,
            );
            return Err(format!("{:?}", e));
        }
        let resp = rslt_cert.unwrap();
        if Decision::Aborted == resp.decision {
            log::debug!("Aborted by talos: xid: {}, operation: 'transfer' {} from {} to {}", xid, amount, from, to);
            return Ok(false);
        }

        log::info!("Running: 'T' {} {} {}", from.number.clone(), amount.clone(), to.number.clone());
        // Check safepoint condition before installing ...

        let safepoint = resp.safepoint.unwrap(); // this is safe for 'Committed'
        log::debug!(
            "... waiting for safepoint: {} on 'T' {} {} {}",
            safepoint,
            from.number.clone(),
            amount.clone(),
            to.number.clone()
        );
        SnapshotApi::await_until_safe(Arc::clone(&self.database), safepoint).await?;

        // install
        log::debug!("... installing 'T' {} {} {}", from.number.clone(), amount.clone(), to.number.clone());
        let rslt = BankApi::transfer(
            Arc::clone(&self.database),
            TransferRequest::new(from.number.clone(), to.number.clone(), amount.clone()),
            resp.version,
        )
        .await;

        if rslt.is_err() {
            let e = rslt.unwrap_err();
            if e.contains("could not serialize access due to concurrent update") {
                log::debug!(
                    "tx conflict when running 'T' {} {} {}. Err: {}, Moving on...\n",
                    from.number.clone(),
                    amount.clone(),
                    to.number.clone(),
                    e
                );
                Ok(true)
            } else {
                Err(e)
            }
        } else {
            log::debug!(
                "updated {} rows when running 'T' {} {} {}\n",
                rslt.unwrap(),
                from.number.clone(),
                amount.clone(),
                to.number.clone()
            );

            Ok(true)
        }
    }
    // $coverage:ignore-end

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

    fn select_snapshot_and_readvers(cpt_snapshot: u64, cpt_versions: Vec<u64>) -> (u64, Vec<u64>) {
        if cpt_versions.is_empty() {
            return (cpt_snapshot, vec![]);
        }

        let mut cpt_version_min: u64 = u64::MAX;
        for v in cpt_versions.iter() {
            if cpt_version_min > *v {
                cpt_version_min = *v;
            }
        }
        let shanpshot_version = std::cmp::max(cpt_snapshot, cpt_version_min);
        let mut read_vers = Vec::<u64>::new();
        for v in cpt_versions.iter() {
            if shanpshot_version < *v {
                read_vers.push(*v);
            }
        }

        (shanpshot_version, read_vers)
    }
}

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use std::assert_ne;
    use std::collections::HashMap;

    use crate::core::Cohort;
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

        let (a1, _, a2, is_deposit) = Cohort::pick(&list);
        assert!(list.get(&a1.number).is_some());
        if let Some(a2_value) = a2 {
            assert!(list.get(&a2_value.number).is_some());
            assert_ne!(*a1, *a2_value);
            assert!(!is_deposit);
        }
    }

    #[test]
    fn select_snapshot_and_readvers() {
        let (s, rv) = Cohort::select_snapshot_and_readvers(1, vec![]);
        assert_eq!(s, 1);
        assert!(rv.is_empty());

        let (s, rv) = Cohort::select_snapshot_and_readvers(1, vec![1]);
        assert_eq!(s, 1);
        assert!(rv.is_empty());

        let (s, rv) = Cohort::select_snapshot_and_readvers(2, vec![1, 2, 2]);
        assert_eq!(s, 2);
        assert!(rv.is_empty());

        let (s, rv) = Cohort::select_snapshot_and_readvers(2, vec![1, 2, 3, 4]);
        assert_eq!(s, 2);
        assert_eq!(rv, vec![3, 4]);

        let (s, rv) = Cohort::select_snapshot_and_readvers(5, vec![1, 2, 3, 4]);
        assert_eq!(s, 5);
        assert!(rv.is_empty());
    }
}
// $coverage:ignore-end
