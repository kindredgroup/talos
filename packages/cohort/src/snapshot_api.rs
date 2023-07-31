use std::sync::Arc;
use std::time::Duration;

use time::OffsetDateTime;
use tokio::task::JoinHandle;
use tokio_postgres::Row;

use crate::model::snapshot::Snapshot;
use crate::state::data_access_api::ManualTx;
use crate::state::postgres::database::{Database, DatabaseError, SNAPSHOT_SINGLETON_ROW_ID};

pub static SNAPSHOT_UPDATE_QUERY: &str = r#"UPDATE cohort_snapshot SET "version" = ($1)::BIGINT WHERE id = $2 AND "version" < ($1)::BIGINT"#;

// #[derive(Debug, Clone)]
pub struct SnapshotApi {}

impl SnapshotApi {
    pub fn from_row(row: &Row) -> Result<Snapshot, DatabaseError> {
        let updated = row
            .try_get::<&str, i64>("version")
            .map_err(|e| DatabaseError::deserialise_payload(e.to_string(), "Cannot read snapshot".into()))?;

        Ok(Snapshot { version: updated as u64 })
    }

    pub async fn query(db: Arc<Database>) -> Result<Snapshot, DatabaseError> {
        db.query_one(
            r#"SELECT "version" FROM cohort_snapshot WHERE id = $1"#,
            &[&SNAPSHOT_SINGLETON_ROW_ID],
            SnapshotApi::from_row,
        )
        .await
    }

    pub async fn update_using<T: ManualTx>(client: &T, new_version: u64) -> Result<u64, String> {
        let mut udpate_was_successful = true;
        let affected_rows = client
            .execute(SNAPSHOT_UPDATE_QUERY.to_string(), &[&(new_version as i64), &SNAPSHOT_SINGLETON_ROW_ID])
            .await?;

        if affected_rows == 0 {
            udpate_was_successful = false;
            log::info!(
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

    pub async fn await_until_safe(db: Arc<Database>, safepoint: u64) -> Result<(Duration, Duration, u64), String> {
        let mut iterations_used = 0_u64;
        let mut s1_db = Duration::from_nanos(0);
        let mut s2_sleeps = Duration::from_nanos(0);
        // quick path...
        let s1_db_s = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let safe_now = Self::is_safe_to_proceed(Arc::clone(&db), safepoint).await?;
        s1_db += Duration::from_nanos((OffsetDateTime::now_utc().unix_timestamp_nanos() - s1_db_s) as u64);
        if safe_now {
            return Ok((s1_db, s2_sleeps, iterations_used));
        }

        // long path...
        let timeout_at = OffsetDateTime::now_utc().unix_timestamp() + 600;
        let poll_frequency = Duration::from_secs(1);
        let started_at = OffsetDateTime::now_utc().unix_timestamp();
        let poll_handle: JoinHandle<Result<(Duration, Duration, u64), String>> = tokio::spawn(async move {
            let db = Arc::clone(&db);
            loop {
                iterations_used += 1;
                let now = OffsetDateTime::now_utc().unix_timestamp();
                let is_timed_out = now >= timeout_at;
                if is_timed_out {
                    break Err(format!("await_until_safe(): Timeout after: {}s", (now - started_at)));
                }

                let s1_db_s = OffsetDateTime::now_utc().unix_timestamp_nanos();
                let is_safe = Self::is_safe_to_proceed(Arc::clone(&db), safepoint).await?;
                s1_db += Duration::from_nanos((OffsetDateTime::now_utc().unix_timestamp_nanos() - s1_db_s) as u64);
                if is_safe {
                    break Ok((s1_db, s2_sleeps, iterations_used));
                }

                let s2_sleeps_s = OffsetDateTime::now_utc().unix_timestamp_nanos();
                tokio::time::sleep(poll_frequency).await;
                s2_sleeps += Duration::from_nanos((OffsetDateTime::now_utc().unix_timestamp_nanos() - s2_sleeps_s) as u64);
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
