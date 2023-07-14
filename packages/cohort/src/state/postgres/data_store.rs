// $coverage:ignore-start
use std::sync::Arc;

use crate::bank_api::BankApi;
use crate::model::bank_account::BankAccount;
use crate::model::snapshot::Snapshot;
use crate::snapshot_api::SnapshotApi;
use crate::state::postgres::database::{Database, SNAPSHOT_SINGLETON_ROW_ID};

use super::database::DatabaseError;

pub struct DataStore {}
impl DataStore {
    pub async fn prefill_snapshot(db: Arc<Database>, snapshot: Snapshot) -> Result<Snapshot, DatabaseError> {
        let rslt = db
            .query_opt(
                r#"SELECT "version" FROM cohort_snapshot WHERE id = $1 AND "version" > $2"#,
                &[&SNAPSHOT_SINGLETON_ROW_ID, &(snapshot.version as i64)],
                SnapshotApi::from_row,
            )
            .await?;

        if let Some(snapshot) = rslt {
            Ok(snapshot)
        } else {
            db.query_one(
                r#"
                    INSERT INTO cohort_snapshot ("id", "version") VALUES ($1, $2)
                    ON CONFLICT(id) DO
                        UPDATE SET version = $2 RETURNING version
                "#,
                &[&SNAPSHOT_SINGLETON_ROW_ID, &(snapshot.version as i64)],
                SnapshotApi::from_row,
            )
            .await
        }
    }

    pub async fn prefill_accounts(db: Arc<Database>, accounts: Vec<BankAccount>) -> Result<Vec<BankAccount>, DatabaseError> {
        let client = db.pool.get().await.unwrap();
        let mut updated_accounts = Vec::<BankAccount>::new();
        for acc in accounts.iter() {
            let updated = {
                let rslt = client
                    .query_opt(
                        r#"SELECT "name", "number", "amount", "version" FROM bank_accounts WHERE "number" = $1 AND "version" >= $2"#,
                        &[&acc.number, &(acc.version as i64)],
                    )
                    .await
                    .unwrap();

                if rslt.is_some() {
                    BankApi::account_from_row(&rslt.unwrap())?
                } else {
                    // update db with new account data
                    let updated_row = client
                        .query_one(
                            r#"
                                INSERT INTO bank_accounts("name", "number", "amount", "version") VALUES ($1, $2, $3, $4)
                                ON CONFLICT(number) DO
                                    UPDATE SET "name" = $1, "amount" = $3, "version" = $4 RETURNING "name", "number", "amount", "version"
                            "#,
                            &[&acc.name, &acc.number, &acc.balance, &(acc.version as i64)],
                        )
                        .await
                        .unwrap();

                    BankApi::account_from_row(&updated_row)?
                }
            };

            updated_accounts.push(updated);
        }

        Ok(updated_accounts)
    }
}
// $coverage:ignore-end
