use std::sync::Arc;
use tokio_postgres::Row;

use tokio_postgres::types::Json;

use crate::model::bank_account::BankAccount;
use crate::state::model::Snapshot;
use crate::state::postgres::database::{Database, SNAPSHOT_SINGLETON_ROW_ID};

pub struct DataStore {}
impl DataStore {
    pub async fn prefill_snapshot(db: Arc<Database>, snapshot: Snapshot) -> Result<Snapshot, String> {
        let rslt = Database::query_opt(
            &db.pool.get().await.unwrap(),
            r#"SELECT "version" FROM cohort_snapshot WHERE id = $1 AND "version" > $2"#,
            &[&SNAPSHOT_SINGLETON_ROW_ID, &(snapshot.version as i64)],
        )
        .await;

        if rslt.is_some() {
            let version = rslt.unwrap().get::<&str, i64>("version");
            Ok(Snapshot { version: version as u64 })
        } else {
            let updated_row = Database::query_one(
                &db.pool.get().await.unwrap(),
                r#"
                    INSERT INTO cohort_snapshot ("id", "version") VALUES ($1, $2)
                    ON CONFLICT(id) DO
                        UPDATE SET version = $2 RETURNING version
                "#,
                &[&SNAPSHOT_SINGLETON_ROW_ID, &(snapshot.version as i64)],
            )
            .await;

            let version = updated_row.get::<&str, i64>("version");
            Ok(Snapshot { version: version as u64 })
        }
    }

    pub async fn prefill_accounts(db: Arc<Database>, accounts: Vec<BankAccount>) -> Result<Vec<BankAccount>, String> {
        let client = db.pool.get().await.unwrap();

        let mut updated_accounts = Vec::<BankAccount>::new();
        for acc in accounts.iter() {
            let updated = {
                let rslt = Database::query_opt(
                    &client,
                    r#"SELECT "number", "data" FROM bank_accounts WHERE "number" = $1 AND (data->'talosState'->'version')::BIGINT >= $2"#,
                    &[&acc.number, &(acc.talos_state.version as i64)],
                )
                .await;

                if rslt.is_some() {
                    Self::account_from_row(&rslt.unwrap())
                } else {
                    // update db with new account data
                    let updated_row = Database::query_one(
                        &client,
                        r#"
                            INSERT INTO bank_accounts("number", "data") VALUES ($1, $2)
                            ON CONFLICT(number) DO
                                UPDATE SET data = $2 RETURNING data
                        "#,
                        &[&acc.number, &Json(acc)],
                    )
                    .await;

                    Self::account_from_row(&updated_row)
                }
            };

            updated_accounts.push(updated);
        }

        Ok(updated_accounts)
    }

    pub fn account_from_row(row: &Row) -> BankAccount {
        row.get::<&str, Json<BankAccount>>("data").0
    }

    pub fn snapshot_from_row(row: &Row) -> Snapshot {
        let updated = row.get::<&str, i64>("version");
        Snapshot { version: updated as u64 }
    }
}
