use std::sync::Arc;

use async_trait::async_trait;
use cohort_banking::{
    callbacks::statemap_installer::StatemapInstallerImpl,
    state::postgres::{database::Database, database_config::DatabaseConfig},
};
use talos_certifier::{env_var, env_var_with_defaults, ports::MessageReciever};
use talos_certifier_adapters::{KafkaConfig, KafkaConsumer};
use talos_cohort_replicator::{talos_cohort_replicator, CohortReplicatorConfig, ReplicatorSnapshot};

use cohort_banking::state::postgres::database::DatabaseError;
use tokio::{signal, try_join};

pub static SNAPSHOT_SINGLETON_ROW_ID: &str = "SINGLETON";

pub struct SnapshotApi {
    db: Arc<Database>,
}

#[async_trait]
impl ReplicatorSnapshot for SnapshotApi {
    async fn get_snapshot(&self) -> Result<u64, String> {
        let client = &self.db.get().await.unwrap();

        let sql = r#"SELECT "version" FROM cohort_snapshot WHERE id = $1"#;

        let _prepare = client
            .prepare_cached(&sql)
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
    let mut kafka_config = KafkaConfig::from_env();
    kafka_config.group_id = "talos-replicator-dev-1".to_string();
    let kafka_consumer = KafkaConsumer::new(&kafka_config);

    // b. Subscribe to topic.
    kafka_consumer.subscribe().await.unwrap();

    // e. Create postgres statemap installer instance.
    let cfg_db = DatabaseConfig {
        database: env_var!("COHORT_PG_DATABASE"),
        host: env_var!("COHORT_PG_HOST"),
        password: env_var!("COHORT_PG_PASSWORD"),
        port: env_var!("COHORT_PG_PORT"),
        user: env_var!("COHORT_PG_USER"),
        pool_size: env_var_with_defaults!("COHORT_PG_POOL_SIZE", usize, 10),
    };
    let database = Database::init_db(cfg_db).await.map_err(|e| e.to_string()).unwrap();

    let pg_statemap_installer = StatemapInstallerImpl {
        database: Arc::clone(&database),
    };

    let config = CohortReplicatorConfig {
        enable_stats: true,
        channel_size: 100_000,
        suffix_capacity: 10_000,
        suffix_prune_threshold: Some(10),
        suffix_minimum_size_on_prune: None,
        certifier_message_receiver_commit_freq_ms: 10_000,
        statemap_queue_cleanup_freq_ms: 10_000,
        statemap_installer_threadpool: 50,
    };

    let snapshot_api = SnapshotApi { db: Arc::clone(&database) };

    let (replicator_handle, statemap_queue_handle, statemap_installer_handle) =
        talos_cohort_replicator(kafka_consumer, Arc::new(pg_statemap_installer), snapshot_api, config).await;

    let all_async_services = tokio::spawn(async move {
        let result = try_join!(replicator_handle, statemap_queue_handle, statemap_installer_handle);
        log::info!("Result from the services ={result:?}");
    });

    tokio::select! {

        _ = all_async_services => {}

        // CTRL + C termination signal
        _ = signal::ctrl_c() => {
            log::info!("Shutting down...");
        }
    }
}
