use std::sync::Arc;

use async_trait::async_trait;
use banking_common::state::postgres::{
    database::{Database, DatabaseError},
    database_config::DatabaseConfig,
};
use banking_replicator::{app::BankingReplicatorApp, statemap_installer::BankStatemapInstaller};
use talos_cohort_replicator::{callbacks::ReplicatorSnapshotProvider, CohortReplicatorConfig};

use talos_common_utils::{env_var, env_var_with_defaults};
use talos_rdkafka_utils::kafka_config::KafkaConfig;
use tokio::signal;

pub static SNAPSHOT_SINGLETON_ROW_ID: &str = "SINGLETON";

pub struct SnapshotApi {
    db: Arc<Database>,
}

#[async_trait]
impl ReplicatorSnapshotProvider for SnapshotApi {
    async fn get_snapshot(&self) -> Result<u64, String> {
        let client = &self.db.get().await.unwrap();

        let sql = r#"SELECT "version" FROM cohort_snapshot WHERE id = $1"#;

        let _prepare = client
            .prepare_cached(sql)
            .await
            .map_err(|e| DatabaseError::prepare(e.to_string(), sql.to_string()).to_string())?;
        let snapshot_row = client.query_one(sql, &[&SNAPSHOT_SINGLETON_ROW_ID]).await.map_err(|e| e.to_string())?;

        let snapshot: i64 = snapshot_row.get("version");

        Ok(snapshot as u64)
    }
}

#[tokio::main]
async fn main() {
    env_logger::builder().format_timestamp_millis().init();

    // 0. Create required items.
    //  a. Create Kafka consumer
    let mut kafka_config = KafkaConfig::from_env(None);
    kafka_config.group_id = env_var!("BANK_REPLICATOR_KAFKA_GROUP_ID");

    // e. Create postgres statemap installer instance.
    let cfg_db = DatabaseConfig {
        database: env_var!("COHORT_PG_DATABASE"),
        host: env_var!("COHORT_PG_HOST"),
        password: env_var!("COHORT_PG_PASSWORD"),
        port: env_var!("COHORT_PG_PORT"),
        user: env_var!("COHORT_PG_USER"),
        pool_size: env_var_with_defaults!("COHORT_PG_POOL_SIZE", u32, 10),
    };
    let database = Database::init_db(cfg_db.clone()).await.map_err(|e| e.to_string()).unwrap();

    let pg_statemap_installer = BankStatemapInstaller {
        database: Arc::clone(&database),
        max_retry: env_var_with_defaults!("BANK_STATEMAP_INSTALLER_MAX_RETRY", u32, 3),
        retry_wait_ms: env_var_with_defaults!("BANK_STATEMAP_INSTALL_RETRY_WAIT_MS", u64, 2),
    };

    let config = CohortReplicatorConfig {
        enable_stats: env_var_with_defaults!("REPLICATOR_ENABLE_STATS", bool, false),
        channel_size: env_var_with_defaults!("REPLICATOR_CHANNEL_SIZE", usize, 100_000),
        suffix_capacity: env_var_with_defaults!("REPLICATOR_SUFFIX_CAPACITY", usize, 10_000),
        suffix_prune_threshold: env_var_with_defaults!("REPLICATOR_SUFFIX_PRUNE_THRESHOLD", Option::<usize>, 1),
        suffix_minimum_size_on_prune: env_var_with_defaults!("REPLICATOR_SUFFIX_MIN_SIZE", Option::<usize>),
        certifier_message_receiver_commit_freq_ms: env_var_with_defaults!("REPLICATOR_COMMIT_FREQ_MS", u64, 10_000),
        statemap_queue_cleanup_freq_ms: env_var_with_defaults!("STATEMAP_QUEUE_CLEANUP_FREQUENCY_MS", u64, 10_000),
        statemap_installer_threadpool: env_var_with_defaults!("STATEMAP_INSTALLER_THREAD_POOL", u64, 50),
    };

    let snapshot_api = SnapshotApi { db: Arc::clone(&database) };

    let app = BankingReplicatorApp::new(kafka_config, cfg_db, config, Arc::new(pg_statemap_installer))
        .await
        .unwrap();
    let handle = app.run(snapshot_api).await.unwrap();

    tokio::select! {

        _ = handle => {}

        // CTRL + C termination signal
        _ = signal::ctrl_c() => {
            log::info!("Shutting down...");
        }
    }
}
