use std::sync::Arc;

use crate::state::model::Snapshot;
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

    pub async fn update(db: Arc<Database>, new_version: u64) -> Result<(), String> {
        let updated = db
            .query_opt(
                r#"UPDATE cohort_snapshot SET "version" = $1 WHERE id = $2 AND "version" < $1 RETURNING "version""#,
                &[&(new_version as i64), &SNAPSHOT_SINGLETON_ROW_ID],
                |_| (),
            )
            .await;

        if updated.is_none() {
            return Err(format!(
                "Could not set 'cohort_snapshot.version' to '{}'. The current version has moved ahead",
                new_version,
            ));
        }

        Ok(())
    }
}
