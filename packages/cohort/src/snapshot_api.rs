use std::sync::Arc;
use std::time::Duration;

use time::OffsetDateTime;
use tokio::task::JoinHandle;

use crate::model::snapshot::Snapshot;
use crate::state::data_access_api::ManualTx;
use crate::state::postgres::data_store::DataStore;
use crate::state::postgres::database::{Database, SNAPSHOT_SINGLETON_ROW_ID};

pub static SNAPSHOT_UPDATE_QUERY: &str = r#"UPDATE cohort_snapshot SET "version" = ($1)::BIGINT WHERE id = $2 AND "version" < ($1)::BIGINT"#;

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

    pub async fn update_using<T: ManualTx>(client: &T, new_version: u64) -> Result<u64, String> {
        let mut udpate_was_successful = true;
        let affected_rows = client
            .execute(SNAPSHOT_UPDATE_QUERY.to_string(), &[&(new_version as i64), &SNAPSHOT_SINGLETON_ROW_ID])
            .await?;

        if affected_rows == 0 {
            udpate_was_successful = false;
            log::warn!(
                "Could not set 'cohort_snapshot.version' to '{}'. The current version has moved ahead",
                new_version,
            );
        }

        if udpate_was_successful {
            log::debug!("updated snapshot to: {}", new_version);
        } else {
            log::debug!("attempted to update snapshot to: {} but query returned rows: {}", new_version, affected_rows);
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
        let timeout_at = OffsetDateTime::now_utc().unix_timestamp() + 60;
        let poll_frequency = Duration::from_secs(1);
        let started_at = OffsetDateTime::now_utc().unix_timestamp();
        let poll_handle: JoinHandle<Result<(), String>> = tokio::spawn(async move {
            let db = Arc::clone(&db);
            loop {
                let now = OffsetDateTime::now_utc().unix_timestamp();
                let is_timed_out = now >= timeout_at;
                if is_timed_out {
                    break Err(format!("await_until_safe(): Timeout after: {}s", (now - started_at)));
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
