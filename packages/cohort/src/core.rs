// $coverage:ignore-start
use std::future::Future;
use std::ops::Sub;
use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use time::OffsetDateTime;

use talos_agent::agent::core::TalosAgentImpl;
use talos_agent::agent::model::{CancelRequestChannelMessage, CertifyRequestChannelMessage};
use talos_agent::api::{AgentConfig, CandidateData, CertificationRequest, CertificationResponse, KafkaConfig, TalosAgent};
use talos_agent::messaging::api::{Decision, DecisionMessage};
use talos_agent::messaging::kafka::KafkaInitializer;
use talos_agent::metrics::client::MetricsClient;
use talos_agent::metrics::core::Metrics;
use talos_agent::metrics::model::Signal;
use talos_agent::mpsc::core::{ReceiverWrapper, SenderWrapper};

use crate::bank_api::BankApi;
use crate::model::bank_account::{as_money, BankAccount};
use crate::replicator::core::StatemapItem;
use crate::snapshot_api::SnapshotApi;
use crate::state::model::{AccountUpdateRequest, BusinessActionType, TransferRequest};
use crate::state::postgres::database::Database;
use crate::tx_batch_executor::BatchExecutor;

// $coverage:ignore-end

pub struct Cohort {
    agent: Box<dyn TalosAgent + Sync + Send>,
    database: Arc<Database>,
}

impl Cohort {
    // $coverage:ignore-start
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
    // $coverage:ignore-end

    // $coverage:ignore-start
    pub fn new(agent: Box<dyn TalosAgent + Sync + Send>, database: Arc<Database>) -> Self {
        Cohort { agent, database }
    }
    // $coverage:ignore-end

    // $coverage:ignore-start
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

        match BatchExecutor::execute(&self.database, batch, Some(version)).await {
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

    pub async fn generate_workload(&self, duration_sec: u32) -> Result<(), String> {
        log::info!("Generating test load for {}s", duration_sec);

        let started_at = OffsetDateTime::now_utc();
        loop {
            let accounts = BankApi::get_accounts(Arc::clone(&self.database)).await?;

            // pick random bank accounts and amount
            let (account1, amount, account2, is_deposit) = Self::pick(&accounts);
            if account2.is_some() {
                // two accounts picked, do transfer...

                // Cohort should do local validation before attempting to alter internal state
                let balance = BankApi::get_balance(Arc::clone(&self.database), account1.number.clone()).await?;

                if balance < as_money(amount.clone(), account1.balance.currency())? {
                    log::warn!("Cannot transfer {:>2} from {} with balance {}", amount, account1.number, balance);
                    continue;
                }

                self.transfer(account1, account2.unwrap(), amount).await?;
                continue;
            }

            // single account picked, do deposit or withdrawal

            if is_deposit {
                self.do_bank(account1, amount, BankApi::deposit).await?;
                continue;
            }

            // Cohort should do local validation before attempting to alter internal state
            let balance = BankApi::get_balance(Arc::clone(&self.database), account1.number.clone()).await?;

            if balance < as_money(amount.clone(), account1.balance.currency())? {
                log::warn!("Cannot withdraw {:>2} from {} with balance {}", amount, account1.number, balance);
                continue;
            }

            self.do_bank(account1, amount, BankApi::withdraw).await?;

            let elapsed = OffsetDateTime::now_utc().sub(started_at).as_seconds_f32();
            if (duration_sec as f32) <= elapsed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let accounts = BankApi::get_accounts(Arc::clone(&self.database)).await?;
        log::info!("New state of bank accounts is");
        for a in accounts.iter() {
            log::info!("{}", a);
        }

        Ok(())
    }
    // $coverage:ignore-end

    fn pick(accounts: &Vec<BankAccount>) -> (&BankAccount, String, Option<&BankAccount>, bool) {
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

    // $coverage:ignore-start
    async fn do_bank<F, R>(&self, account: &BankAccount, amount: String, op_impl: F) -> Result<(), String>
    where
        F: Fn(Arc<Database>, AccountUpdateRequest, u64) -> R,
        R: Future<Output = Result<u64, String>>,
    {
        let snapshot = SnapshotApi::query(Arc::clone(&self.database)).await?;

        let xid = uuid::Uuid::new_v4().to_string();
        let cert_req = CertificationRequest {
            message_key: "cohort-sample".to_string(),
            candidate: CandidateData {
                xid: xid.clone(),
                readset: vec![account.number.to_string()],
                readvers: vec![account.talos_state.version],
                snapshot: snapshot.version,
                writeset: vec![account.number.to_string()],
            },
            timeout: Some(Duration::from_secs(10)),
        };

        let rslt_cert = self.agent.certify(cert_req).await;
        if let Err(e) = rslt_cert {
            log::warn!(
                "Error communicating via agent: {}, xid: {}, operation: 'deposit/withdraw' {} to {}",
                e.reason,
                xid,
                amount,
                account,
            );
            return Ok(());
        }
        let resp = rslt_cert.unwrap();
        if Decision::Aborted == resp.decision {
            log::warn!("Aborted by talos: xid: {}, operation: 'deposit' {} to {}", xid, amount, account);
            return Ok(());
        }

        // Talos gave "go ahead"

        // Check safepoint condition before installing ...

        let safepoint = resp.safepoint.unwrap(); // this is safe for 'Committed'
        SnapshotApi::await_until_safe(Arc::clone(&self.database), safepoint).await?;

        // install

        op_impl(
            Arc::clone(&self.database),
            AccountUpdateRequest::new(account.number.clone(), amount),
            resp.version,
        )
        .await?;

        // TODO: this should be done by replicator
        SnapshotApi::update(Arc::clone(&self.database), resp.version).await?;

        Ok(())
    }
    // $coverage:ignore-end

    // $coverage:ignore-start
    async fn transfer(&self, from: &BankAccount, to: &BankAccount, amount: String) -> Result<(), String> {
        // todo Implement snapshot tracking
        let snapshot = SnapshotApi::query(Arc::clone(&self.database)).await?;

        let xid = uuid::Uuid::new_v4().to_string();
        let cert_req = CertificationRequest {
            message_key: "cohort-sample".to_string(),
            candidate: CandidateData {
                xid: xid.clone(),
                readset: vec![from.number.to_string(), to.number.to_string()],
                readvers: vec![from.talos_state.version, to.talos_state.version],
                snapshot: snapshot.version,
                writeset: vec![from.number.to_string(), to.number.to_string()],
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
            return Ok(());
        }
        let resp = rslt_cert.unwrap();
        if Decision::Aborted == resp.decision {
            log::warn!("Aborted by talos: xid: {}, operation: 'transfer' {} from {} to {}", xid, amount, from, to);
            return Ok(());
        }

        // Check safepoint condition before installing ...

        let safepoint = resp.safepoint.unwrap(); // this is safe for 'Committed'
        SnapshotApi::await_until_safe(Arc::clone(&self.database), safepoint).await?;

        // install

        BankApi::transfer(Arc::clone(&self.database), from.number.clone(), to.number.clone(), amount.clone(), resp.version).await?;

        // TODO: this should be done by replicator
        SnapshotApi::update(Arc::clone(&self.database), resp.version).await?;

        Ok(())
    }
    // $coverage:ignore-end
}

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use std::{assert_ne, vec};

    use crate::core::Cohort;
    use crate::model::bank_account::BankAccount;
    use crate::model::talos_state::TalosState;

    #[test]
    fn pick() {
        let list = vec![
            BankAccount::aud("a1".to_string(), "a1".to_string(), "1".to_string(), TalosState { version: 1 }),
            BankAccount::aud("a2".to_string(), "a2".to_string(), "2".to_string(), TalosState { version: 2 }),
            BankAccount::aud("a3".to_string(), "a3".to_string(), "3".to_string(), TalosState { version: 3 }),
        ];

        let (a1, _, a2, is_deposit) = Cohort::pick(&list);
        assert!(list.contains(a1));
        if a2.is_some() {
            assert!(list.contains(a2.unwrap()));
            assert_ne!(*a1, *a2.unwrap());
            assert!(!is_deposit);
        }
    }
}
// $coverage:ignore-end
