use std::sync::Arc;

use banking_common::state::postgres::{database::Database, database_config::DatabaseConfig};
use talos_certifier::ports::MessageReciever;
use talos_certifier_adapters::KafkaConsumer;
use talos_cohort_replicator::{
    callbacks::{ReplicatorInstaller, ReplicatorSnapshotProvider},
    talos_cohort_replicator, CohortReplicatorConfig,
};
use talos_rdkafka_utils::kafka_config::KafkaConfig;
use tokio::{sync::mpsc, task::JoinHandle};

type InstallerType = dyn ReplicatorInstaller + Send + Sync;

pub struct BankingReplicatorApp {
    pub database: Arc<Database>,
    kafka_config: KafkaConfig,
    config: CohortReplicatorConfig,
    statemap_installer: Arc<InstallerType>,
}

impl BankingReplicatorApp {
    pub async fn new(
        kafka_config: KafkaConfig,
        db_config: DatabaseConfig,
        config: CohortReplicatorConfig,
        statemap_installer: Arc<InstallerType>,
    ) -> Result<Self, String> {
        Ok(BankingReplicatorApp {
            database: Database::init_db(db_config).await.map_err(|e| e.to_string())?,
            kafka_config,
            config,
            statemap_installer,
        })
    }

    pub async fn run<S>(&self, snapshot_provider: S) -> Result<JoinHandle<()>, String>
    where
        S: ReplicatorSnapshotProvider + Send + Sync + 'static,
    {
        let kafka_consumer = KafkaConsumer::new(&self.kafka_config);

        let statemap_channel = mpsc::channel(50_000);

        // b. Subscribe to topic.
        kafka_consumer.subscribe().await.map_err(|e| e.to_string())?;

        let installer = Arc::clone(&self.statemap_installer);
        let cfg = self.config.clone();

        let service_handle = tokio::spawn(async move {
            let result = talos_cohort_replicator(kafka_consumer, installer, snapshot_provider, statemap_channel, cfg).await;
            log::info!("Result from the services ={result:?}");
        });

        Ok(service_handle)
    }
}
