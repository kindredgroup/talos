use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use cohort_sdk::{
    cohort::Cohort,
    model::{CandidateData, CertificationRequest, ClientErrorKind, Config},
};
use opentelemetry_api::{
    global,
    metrics::{Counter, Unit},
    Context,
};
use talos_agent::messaging::api::Decision;

use crate::{
    callbacks::{oo_installer::OutOfOrderInstallerImpl, state_provider::StateProviderImpl},
    examples_support::queue_processor::Handler,
    model::requests::{BusinessActionType, TransferRequest},
    state::postgres::{database::Database, database_config::DatabaseConfig},
};

pub struct BankingApp {
    config: Config,
    cohort_api: Option<cohort_sdk::cohort::Cohort>,
    pub database: Arc<Database>,
    counter_aborts: Arc<Counter<u64>>,
    counter_commits: Arc<Counter<u64>>,
    counter_oo_no_data_found: Arc<Counter<u64>>,
}

impl BankingApp {
    pub async fn new(config: Config) -> Result<Self, String> {
        let db_config = DatabaseConfig {
            pool_size: config.db_pool_size,
            user: config.db_user.clone(),
            password: config.db_password.clone(),
            host: config.db_host.clone(),
            port: config.db_port.clone(),
            database: config.db_database.clone(),
        };

        let meter = global::meter("banking_cohort");
        let counter_aborts = meter.u64_counter("metric_aborts").with_unit(Unit::new("tx")).init();
        let counter_commits = meter.u64_counter("metric_commits").with_unit(Unit::new("tx")).init();
        let counter_oo_no_data_found = meter.u64_counter("metric_oo_no_data_found").with_unit(Unit::new("tx")).init();

        Ok(BankingApp {
            config: config.clone(),
            cohort_api: None,
            database: Database::init_db(db_config).await.map_err(|e| e.to_string())?,
            counter_aborts: Arc::new(counter_aborts),
            counter_commits: Arc::new(counter_commits),
            counter_oo_no_data_found: Arc::new(counter_oo_no_data_found),
        })
    }

    pub async fn init(&mut self) -> Result<(), String> {
        let cohort_api = Cohort::create(self.config.clone()).await.map_err(|e| e.to_string())?;

        self.cohort_api = Some(cohort_api);

        Ok(())
    }
}

#[async_trait]
impl Handler<TransferRequest> for BankingApp {
    async fn handle(&self, request: TransferRequest) -> Result<(), String> {
        log::debug!("processig new banking transfer request: {:?}", request);

        let request_copy = request.clone();

        let statemap = vec![HashMap::from([(
            BusinessActionType::TRANSFER.to_string(),
            TransferRequest::new(request.from.clone(), request.to.clone(), request.amount).json(),
        )])];

        let request = CertificationRequest {
            timeout_ms: 0,
            candidate: CandidateData {
                readset: vec![request.from.clone(), request.to.clone()],
                writeset: vec![request.from, request.to],
                statemap: Some(statemap),
            },
        };

        let single_query_strategy = true;
        let state_provider = StateProviderImpl {
            database: Arc::clone(&self.database),
            request: request_copy.clone(),
            single_query_strategy,
        };

        let oo_inst = OutOfOrderInstallerImpl {
            database: Arc::clone(&self.database),
            request: request_copy,
            detailed_logging: false,
            counter_oo_no_data_found: Arc::clone(&self.counter_oo_no_data_found),
            single_query_strategy,
        };

        match self
            .cohort_api
            .as_ref()
            .expect("Banking app is not initialised")
            .certify(request, &state_provider, &oo_inst)
            .await
        {
            Ok(rsp) => {
                let ca = Arc::clone(&self.counter_aborts);
                let cc = Arc::clone(&self.counter_commits);
                let is_abort = rsp.decision == Decision::Aborted;
                tokio::spawn(async move {
                    if is_abort {
                        ca.add(&Context::current(), 1, &[]);
                    } else {
                        cc.add(&Context::current(), 1, &[]);
                    }
                });

                log::debug!("Talos decision for xid '{}' is: {:?}", rsp.xid, rsp.decision);
                Ok(())
            }
            Err(client_error) => match client_error.kind {
                ClientErrorKind::OutOfOrderSnapshotTimeout => Ok(()),
                _ => Err(client_error.to_string()),
            },
        }
    }
}
