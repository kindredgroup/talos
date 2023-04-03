use std::ops::Sub;
use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use time::OffsetDateTime;
use tokio::sync::mpsc::Sender;

use talos_agent::agent::core::TalosAgentImpl;
use talos_agent::agent::model::{CancelRequestChannelMessage, CertifyRequestChannelMessage};
use talos_agent::api::{AgentConfig, CertificationResponse, KafkaConfig, TalosAgent};
use talos_agent::messaging::api::DecisionMessage;
use talos_agent::messaging::kafka::KafkaInitializer;
use talos_agent::metrics::client::MetricsClient;
use talos_agent::metrics::core::Metrics;
use talos_agent::metrics::model::Signal;
use talos_agent::mpsc::core::{ReceiverWrapper, SenderWrapper};

use crate::bank::Bank;
use crate::model::bank_account::{as_money, BankAccount};
use crate::state::model::{AccountOperation, AccountRef, Envelope, OperationResponse};
use crate::state::state_manager::StateManager;

pub struct Cohort {
    _agent: Box<dyn TalosAgent + Sync + Send>,
    tx_state: Option<Sender<Envelope<AccountOperation, OperationResponse>>>,
}

impl Cohort {
    pub async fn make_agent(config: AgentConfig, kafka_config: KafkaConfig) -> Box<dyn TalosAgent + Sync + Send> {
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

    pub fn new(_agent: Box<dyn TalosAgent + Sync + Send>) -> Self {
        Cohort { _agent, tx_state: None }
    }

    /** Start generating the workload */
    pub async fn start(&mut self) {
        log::info!("---------------------");
        let (tx_state, rx_state) = tokio::sync::mpsc::channel::<Envelope<AccountOperation, OperationResponse>>(10_000);
        self.tx_state = Some(tx_state);

        tokio::spawn(async move {
            let initial_state = include_str!("initial_state.json");
            let accounts: Vec<BankAccount> = serde_json::from_str(initial_state)
                .map_err(|e| {
                    log::error!("Unable to read initial data: {}", e);
                })
                .unwrap();

            log::info!("Loaded initial state");
            for a in accounts.iter() {
                log::info!("{}", a);
            }

            let mut state_manager = StateManager { accounts };
            state_manager.run(rx_state).await;
        });
    }

    fn get_tx(&self) -> Result<Sender<Envelope<AccountOperation, OperationResponse>>, String> {
        if self.tx_state.is_none() {
            Err("Cohort is not initialised! Call ::start()".to_string())
        } else {
            Ok(self.tx_state.clone().unwrap())
        }
    }

    pub async fn generate_workload(&self, duration_sec: u32) -> Result<(), String> {
        let accounts = Bank::get_accounts(self.get_tx()?).await?;
        log::info!("Current state of bank accounts is");
        for a in accounts.iter() {
            log::info!("{}", a);
        }

        log::info!("Generating test load for {}s", duration_sec);

        let started_at = OffsetDateTime::now_utc();
        loop {
            // pick random bank accounts and amount
            let (account1, amount, account2, is_deposit) = Self::pick(&accounts);
            if account2.is_some() {
                // two accounts picked, do transfer...

                // Cohort should do local validation before attempting to alter internal state
                let balance = Bank::get_balance(
                    self.get_tx()?,
                    AccountRef {
                        number: account1.number.clone(),
                        new_version: None,
                    },
                )
                .await?;

                if balance.lt(&as_money(amount.clone(), account1.balance.currency())?) {
                    log::warn!("Cannot transfer {:>2} from {} with balance {}", amount, account1.number, balance);
                    continue;
                }

                Bank::transfer(self.get_tx()?, account1, account2.unwrap(), amount).await?;
                continue;
            }

            // single account picked, do deposit or withdrawal

            if is_deposit {
                Bank::deposit(self.get_tx()?, account1, amount).await?;
                continue;
            }

            // Cohort should do local validation before attempting to alter internal state
            let balance = Bank::get_balance(
                self.get_tx()?,
                AccountRef {
                    number: account1.number.clone(),
                    new_version: None,
                },
            )
            .await?;

            if balance.lt(&as_money(amount.clone(), account1.balance.currency())?) {
                log::warn!("Cannot withdraw {:>2} from {} with balance {}", amount, account1.number, balance);
                continue;
            }

            Bank::withdraw(self.get_tx()?, account1, amount).await?;

            let elapsed = OffsetDateTime::now_utc().sub(started_at).as_seconds_f32();
            if (duration_sec as f32) <= elapsed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let accounts = Bank::get_accounts(self.get_tx()?).await?;
        log::info!("New state of bank accounts is");
        for a in accounts.iter() {
            log::info!("{}", a);
        }

        Ok(())
    }

    fn pick(accounts: &Vec<BankAccount>) -> (&BankAccount, String, Option<&BankAccount>, bool) {
        let mut rnd = rand::thread_rng();
        let i = rnd.gen_range(0..accounts.len());
        let account1 = accounts.get(i).unwrap();
        let amount = rnd.gen_range(1..20).to_string();
        if rnd.gen::<bool>() {
            loop {
                let j = rnd.gen_range(0..accounts.len());
                if i == j {
                    continue;
                }
                let account2 = accounts.get(i).unwrap();
                break (account1, amount, Some(account2), false);
            }
        } else {
            (account1, amount, Option::<&BankAccount>::None, rnd.gen::<bool>())
        }
    }
}
