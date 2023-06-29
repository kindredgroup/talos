// $coverage:ignore-start
use std::{io::Error, time::Duration};

use async_trait::async_trait;
use log::debug;
use metrics::model::{MicroMetrics, MinMax};

use crate::{state::postgres::data_access::PostgresApi, tx_batch_executor::BatchExecutor};

use super::core::{ReplicatorInstaller, StatemapItem};

pub struct PgReplicatorStatemapInstaller {
    pub metrics_frequency: Option<i128>,
    pub pg: PostgresApi,
    pub metrics: MicroMetrics,
    pub m_total: MinMax,
    pub m1_tx: MinMax,
    pub m2_exec: MinMax,
    pub m3_ver: MinMax,
    pub m4_snap: MinMax,
    pub m5_commit: MinMax,
}

#[async_trait]
impl ReplicatorInstaller for PgReplicatorStatemapInstaller {
    async fn install(&mut self, sm: Vec<StatemapItem>, version: Option<u64>) -> Result<bool, Error> {
        debug!("Last version ... {:#?} ", version);
        debug!("Original statemaps received ... {:#?} ", sm);
        self.metrics.clock_start();
        let result = BatchExecutor::execute_instrumented(&mut self.pg, sm, version).await;
        let elapsed = self.metrics.clock_end();

        match result {
            Ok(data) => {
                let (s_total, tx, exec, ver, snap, commit) = data.1;
                // Total duration
                self.m_total.add(s_total.1 - s_total.0);
                self.m1_tx.add(tx.1 - tx.0);
                self.m2_exec.add(exec.1 - exec.0);
                self.m3_ver.add(ver.1 - ver.0);
                self.m4_snap.add(snap.1 - snap.0);
                self.m5_commit.add(commit.1 - commit.0);

                if let Some(frequency) = self.metrics_frequency {
                    if elapsed >= frequency {
                        self.metrics.sample_end();
                        log::warn!("METRIC (batch-executor-header): count,total min,total max,tx min,tx max,exec min,exec max,ver min,ver max,snap min,snap max,commit min,commit max,'-',total duration (mcs), tx duration (mcs), exec duration (mcs),ver duration (mcs),snap duration (mcs),commit duration (mcs)");
                        log::warn!(
                            "METRIC (batch-executor): {},{},{},{},{},{},{},{},{},{},{},{},{},'-',{},{},{},{},{},{},",
                            self.m_total.count,
                            Duration::from_nanos(self.m_total.min as u64).as_micros(),
                            Duration::from_nanos(self.m_total.max as u64).as_micros(),
                            Duration::from_nanos(self.m1_tx.min as u64).as_micros(),
                            Duration::from_nanos(self.m1_tx.max as u64).as_micros(),
                            Duration::from_nanos(self.m2_exec.min as u64).as_micros(),
                            Duration::from_nanos(self.m2_exec.max as u64).as_micros(),
                            Duration::from_nanos(self.m3_ver.min as u64).as_micros(),
                            Duration::from_nanos(self.m3_ver.max as u64).as_micros(),
                            Duration::from_nanos(self.m4_snap.min as u64).as_micros(),
                            Duration::from_nanos(self.m4_snap.max as u64).as_micros(),
                            Duration::from_nanos(self.m5_commit.min as u64).as_micros(),
                            Duration::from_nanos(self.m5_commit.max as u64).as_micros(),
                            Duration::from_nanos(self.m_total.sum as u64).as_micros(),
                            Duration::from_nanos(self.m1_tx.sum as u64).as_micros(),
                            Duration::from_nanos(self.m2_exec.sum as u64).as_micros(),
                            Duration::from_nanos(self.m3_ver.sum as u64).as_micros(),
                            Duration::from_nanos(self.m4_snap.sum as u64).as_micros(),
                            Duration::from_nanos(self.m5_commit.sum as u64).as_micros(),
                        );

                        self.m_total.reset();
                        self.m1_tx.reset();
                        self.m2_exec.reset();
                        self.m3_ver.reset();
                        self.m4_snap.reset();
                        self.m5_commit.reset();
                    }
                }
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }
}
// $coverage:ignore-end
