use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use banking_common::{
    model::{BusinessActionType, TransferRequest},
    state::postgres::{database::Database, database_config::DatabaseConfig},
};
use cohort_sdk::{
    cohort::Cohort,
    model::{ClientErrorKind, Config},
};

use opentelemetry_api::{
    global,
    metrics::{Counter, Unit},
};
use serde_json::json;
use talos_agent::messaging::api::Decision;

use crate::{
    callbacks::{certification_candidate_provider::CertificationCandidateProviderImpl, oo_installer::OutOfOrderInstallerImpl},
    examples_support::queue_processor::Handler,
    model::requests::{CandidateData, CertificationRequest},
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
    pub async fn new(config: Config, db_config: DatabaseConfig) -> Result<Self, String> {
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
        // if no metrics are reported to meter then it will not be visible in the final report.
        self.counter_aborts.add(0, &[]);
        self.counter_commits.add(0, &[]);
        self.counter_oo_no_data_found.add(0, &[]);

        self.cohort_api = Some(cohort_api);

        Ok(())
    }
}

#[async_trait]
impl Handler<TransferRequest> for BankingApp {
    async fn handle(&self, request: TransferRequest) -> Result<(), String> {
        log::debug!("processing new banking transfer request: {:?}", request);

        let statemap = vec![HashMap::from([(
            BusinessActionType::TRANSFER.to_string(),
            TransferRequest::new(request.from.clone(), request.to.clone(), request.amount).json(),
        )])];

        let on_commit_value = json!({
            "publish": {
                "kafka": [
                    {
                        "topic": "test.transfer.feedback",
                        "value": {
                            "from_account": request.from,
                            "to_account": request.to,
                            "amount": request.amount
                        }
                    },
                ],
            }
        });

        let certification_request = CertificationRequest {
            timeout_ms: 0,
            candidate: CandidateData {
                readset: vec![request.from.clone(), request.to.clone()],
                writeset: vec![request.from, request.to],
                statemap: Some(statemap),
                on_commit: Some(on_commit_value),
            },
        };

        let single_query_strategy = true;
        let state_provider = CertificationCandidateProviderImpl {
            database: Arc::clone(&self.database),
            single_query_strategy,
        };

        let request_payload_callback = || state_provider.get_certification_candidate(certification_request.clone());

        let oo_inst = OutOfOrderInstallerImpl {
            database: Arc::clone(&self.database),
            detailed_logging: false,
            counter_oo_no_data_found: Arc::clone(&self.counter_oo_no_data_found),
            single_query_strategy,
        };

        match self
            .cohort_api
            .as_ref()
            .expect("Banking app is not initialised")
            .certify(&request_payload_callback, &oo_inst)
            .await
        {
            Ok(rsp) => {
                let c_aborts = Arc::clone(&self.counter_aborts);
                let c_commits = Arc::clone(&self.counter_commits);
                let is_abort = rsp.decision == Decision::Aborted;
                tokio::spawn(async move {
                    if is_abort {
                        c_aborts.add(1, &[]);
                    } else {
                        c_commits.add(1, &[]);
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
