use std::sync::Arc;

use async_trait::async_trait;
use banking_common::state::postgres::{
    database::{Database, DatabaseError, SNAPSHOT_SINGLETON_ROW_ID},
    database_config::DatabaseConfig,
};
use banking_replicator::{app::BankingReplicatorApp, statemap_installer::BankStatemapInstaller};
use talos_cohort_replicator::{callbacks::ReplicatorSnapshotProvider, otel::otel_config::ReplicatorOtelConfig, CohortReplicatorConfig};

use talos_common_utils::{backpressure::config::BackPressureConfig, env_var, env_var_with_defaults};
use talos_rdkafka_utils::kafka_config::KafkaConfig;
use tokio::signal;

pub struct SnapshotApi {
    db: Arc<Database>,
}

const SNAPSHOT_UPDATE_QUERY: &str = r#"UPDATE cohort_snapshot SET "version" = ($1)::BIGINT WHERE id = $2 AND "version" < ($1)::BIGINT"#;

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

    async fn update_snapshot(&self, version: u64) -> Result<(), String> {
        let client = &self.db.get().await.unwrap();

        let _prepare = client
            .prepare_cached(SNAPSHOT_UPDATE_QUERY)
            .await
            .map_err(|e| DatabaseError::prepare(e.to_string(), SNAPSHOT_UPDATE_QUERY.to_string()).to_string())?;
        let _ = client
            .execute(SNAPSHOT_UPDATE_QUERY, &[&(version as i64), &SNAPSHOT_SINGLETON_ROW_ID])
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
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
    let database = Database::init_db(cfg_db.clone())
        .await
        .map_err(|e| e.to_string())
        .expect("Unable to initialise database");

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
        otel_telemetry: ReplicatorOtelConfig {
            init_otel: true,
            enable_metrics: false,
            enable_traces: false,
            name: "example_replicator".into(),
            meter_name: "example_replicator".into(),
            grpc_endpoint: None,
        },
        backpressure: BackPressureConfig::from_env(),
    };

    let snapshot_api = SnapshotApi { db: Arc::clone(&database) };

    let app = BankingReplicatorApp::new(kafka_config, cfg_db, config, Arc::new(pg_statemap_installer))
        .await
        .expect("Unable to create an instance of Banking Replicator");

    let h_replicator = app.run(snapshot_api).await.unwrap();

    tokio::select! {

        _ = h_replicator => {}

        // CTRL + C termination signal
        _ = signal::ctrl_c() => {
            log::info!("Shutting down...");
        }
    }
}
