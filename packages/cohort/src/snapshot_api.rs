use std::sync::Arc;
use std::time::Duration;

use deadpool_postgres::GenericClient;
use time::OffsetDateTime;
use tokio::task::JoinHandle;

use crate::model::snapshot::Snapshot;
use crate::state::postgres::data_store::DataStore;
use crate::state::postgres::database::{Database, SNAPSHOT_SINGLETON_ROW_ID};

pub struct SnapshotApi {}

impl SnapshotApi {
    pub async fn query(db: Arc<Database>) -> Result<Snapshot, String> {
        let result = db
            .query_one(
                r#"SELECT "version" FROM cohort_snapshot WHERE id = $1"#,
                &[&SNAPSHOT_SINGLETON_ROW_ID],
                DataStore::snapshot_from_row,
            )
            .await;
        Ok(result)
    }

    pub async fn update(db: Arc<Database>, new_version: u64) -> Result<u64, String> {
        let updated = db
            .execute(
                r#"UPDATE cohort_snapshot SET "version" = $1 WHERE id = $2 AND "version" < $1"#,
                &[&(new_version as i64), &SNAPSHOT_SINGLETON_ROW_ID],
            )
            .await;

        if updated == 0 {
            return Err(format!(
                "Could not set 'cohort_snapshot.version' to '{}'. The current version has moved ahead",
                new_version,
            ));
        }

        Ok(updated)
    }

    pub async fn update_using<T: GenericClient + Sync>(client: &T, new_version: u64) -> Result<u64, String> {
        let statement = client
            .prepare_cached(r#"UPDATE cohort_snapshot SET "version" = $1 WHERE id = $2 AND "version" < $1"#)
            .await
            .unwrap();

        let affected_rows = client
            .execute(&statement, &[&(new_version as i64), &SNAPSHOT_SINGLETON_ROW_ID])
            .await
            .map_err(|e| e.to_string())?;

        if affected_rows == 0 {
            return Err(format!(
                "Could not set 'cohort_snapshot.version' to '{}'. The current version has moved ahead",
                new_version,
            ));
        }

        Ok(affected_rows)
    }

    pub async fn await_until_safe(db: Arc<Database>, safepoint: u64) -> Result<(), String> {
        // quick path...
        let safe_now = Self::is_safe_to_proceed(Arc::clone(&db), safepoint).await?;
        if safe_now {
            return Ok(());
        }

        // long path...
        let timeout_at = OffsetDateTime::now_utc().unix_timestamp() + 10;
        let poll_frequency = Duration::from_secs(1);

        let poll_handle: JoinHandle<Result<(), String>> = tokio::spawn(async move {
            let db = Arc::clone(&db);
            loop {
                let is_timed_out = OffsetDateTime::now_utc().unix_timestamp() >= timeout_at;
                if is_timed_out {
                    break Err(format!("Timeout afeter: {}s", timeout_at));
                }

                if Self::is_safe_to_proceed(Arc::clone(&db), safepoint).await? {
                    break Ok(());
                }

                tokio::time::sleep(poll_frequency).await;
            }
        });

        poll_handle.await.map_err(|e| e.to_string())?
    }

    async fn is_safe_to_proceed(db: Arc<Database>, safepoint: u64) -> Result<bool, String> {
        match SnapshotApi::query(db).await {
            Err(e) => Err(format!("Unable to read snapshot, please retry... {}", e)),
            Ok(snapshot) => Ok(Snapshot::is_safe_for(snapshot, safepoint)),
        }
    }
}
