// $coverage:ignore-start
use std::io::Error;

use async_trait::async_trait;
use log::{debug, info};

use crate::{state::postgres::data_access::PostgresApi, tx_batch_executor::BatchExecutor};

use super::core::{ReplicatorInstaller, StatemapItem};

pub struct PgReplicatorStatemapInstaller {
    pub pg: PostgresApi,
}

#[async_trait]
impl ReplicatorInstaller for PgReplicatorStatemapInstaller {
    async fn install(&mut self, sm: Vec<StatemapItem>, version: Option<u64>) -> Result<bool, Error> {
        debug!("Last version ... {:#?} ", version);
        debug!("Original statemaps received ... {:#?} ", sm);

        let result = BatchExecutor::execute(&mut self.pg, sm, version).await;

        info!("Result on executing the statmaps is ... {result:?}");

        Ok(result.is_ok())
    }
}
// $coverage:ignore-end
