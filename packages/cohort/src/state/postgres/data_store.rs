use std::sync::Arc;

use deadpool_postgres::GenericClient;
use tokio_postgres::types::{Json, ToSql};
use tokio_postgres::Row;

use crate::model::bank_account::BankAccount;
use crate::state::model::{AccountRef, Snapshot};
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
                    Self::bank_account_from_row(&rslt.unwrap())
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

                    Self::bank_account_from_row(&updated_row)
                }
            };

            updated_accounts.push(updated);
        }

        Ok(updated_accounts)
    }

    pub async fn get_accounts(db: Arc<Database>) -> Result<Vec<BankAccount>, String> {
        let rslt = Database::query(&db.pool.get().await.unwrap(), "SELECT data FROM bank_accounts", &[]).await;
        let list: Vec<BankAccount> = rslt.iter().map(Self::bank_account_from_row).collect::<Vec<BankAccount>>();
        Ok(list)
    }

    pub async fn get_account(db: Arc<Database>, number: String) -> Result<Option<BankAccount>, String> {
        let rslt = Database::query_opt(&db.pool.get().await.unwrap(), "SELECT data FROM bank_accounts WHERE number = $1", &[&number]).await;
        Ok(rslt.map(|r| Self::bank_account_from_row(&r)))
    }

    pub async fn deposit(db: Arc<Database>, amount: &str, account_ref: AccountRef) -> Result<BankAccount, String> {
        let client = db.pool.get().await.unwrap();
        let updated = if let Some(new_version) = account_ref.new_version {
            let sql = r#"
                UPDATE bank_accounts SET data = data ||
                jsonb_build_object(
                    'amount', ((data->>'amount')::DECIMAL + (($1)::TEXT)::DECIMAL)::TEXT,
                    'talosState', jsonb_build_object('version', ($2)::BIGINT)
                )
                WHERE "number" = $3
                RETURNING data
            "#;
            Database::query_one(&client, sql, &[&amount, &(new_version as i64), &account_ref.number]).await
        } else {
            let sql = r#"
                UPDATE bank_accounts SET data = data ||
                jsonb_build_object(
                    'amount', ((data->>'amount')::DECIMAL + (($1)::TEXT)::DECIMAL)::TEXT
                )
                WHERE "number" = $2
                RETURNING data
            "#;
            Database::query_one(&client, sql, &[&amount, &account_ref.number]).await
        };

        Ok(Self::bank_account_from_row(&updated))
    }

    pub async fn withdraw(db: Arc<Database>, amount: &str, account_ref: AccountRef) -> Result<BankAccount, String> {
        Self::deposit(db, &format!("-{}", amount), account_ref).await
    }

    pub async fn transfer(db: Arc<Database>, amount: &str, from: AccountRef, to: AccountRef) -> Result<(), String> {
        let sql = format!(
            r#"
            DO $$
            DECLARE
                amount          DECIMAL         := ('{}')::DECIMAL;
                from_account    VARCHAR(255)    := '{}';
                from_ver        BIGINT          := ('{}')::BIGINT;
                to_account      VARCHAR(255)    := '{}';
                to_ver          BIGINT          := ('{}')::BIGINT;
            BEGIN
                UPDATE bank_accounts SET data = data ||
                jsonb_build_object(
                    'amount', ((data->>'amount')::DECIMAL + amount)::TEXT,
                    'talosState', jsonb_build_object('version', to_ver)
                )
                WHERE "number" = to_account;

                UPDATE bank_accounts SET data = data ||
                jsonb_build_object(
                    'amount', ((data->>'amount')::DECIMAL - amount)::TEXT,
                    'talosState', jsonb_build_object('version', from_ver)
                )
                WHERE "number" = from_account;
            END$$
            "#,
            amount,
            from.number,
            from.new_version.unwrap(),
            to.number,
            to.new_version.unwrap(),
        );

        let params: &[&(dyn ToSql + Sync)] = &[];
        let client = db.pool.get().await.unwrap();
        let statement = client.prepare(&sql).await.unwrap();
        let rslt = client.execute(&statement, params).await;

        match rslt {
            Err(e) => Err(e.to_string()),
            Ok(_) => Ok(()),
        }
    }

    pub async fn update_snapshot(db: Arc<Database>, new_version: u64) -> Result<Snapshot, String> {
        let updated = Database::query_opt(
            &db.pool.get().await.unwrap(),
            r#"UPDATE cohort_snapshot SET "version" = $1 WHERE id = $2 AND "version" < $1 RETURNING "version""#,
            &[&(new_version as i64), &SNAPSHOT_SINGLETON_ROW_ID],
        )
        .await;

        if updated.is_none() {
            return Err(format!(
                "Could not set 'cohort_snapshot.version' to '{}'. The current version has moved ahead",
                new_version,
            ));
        }

        Ok(Self::snapshot_from_row(&updated.unwrap()))
    }

    pub async fn get_snapshot(db: Arc<Database>) -> Result<Snapshot, String> {
        let row = Database::query_one(
            &db.pool.get().await.unwrap(),
            r#"SELECT "version" FROM cohort_snapshot WHERE id = $1"#,
            &[&SNAPSHOT_SINGLETON_ROW_ID],
        )
        .await;

        Ok(Self::snapshot_from_row(&row))
    }

    fn bank_account_from_row(row: &Row) -> BankAccount {
        row.get::<&str, Json<BankAccount>>("data").0
    }

    fn snapshot_from_row(row: &Row) -> Snapshot {
        let updated = row.get::<&str, i64>("version");
        Snapshot { version: updated as u64 }
    }
}
