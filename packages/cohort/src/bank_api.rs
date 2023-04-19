use std::sync::Arc;

use rusty_money::iso::Currency;
use rusty_money::Money;
use tokio_postgres::types::ToSql;

use crate::model::bank_account::BankAccount;
use crate::state::model::AccountRef;
use crate::state::postgres::data_store::DataStore;
use crate::state::postgres::database::Database;

pub struct BankApi {}

impl BankApi {
    pub async fn get_accounts(db: Arc<Database>) -> Result<Vec<BankAccount>, String> {
        let list = db.query("SELECT data FROM bank_accounts", DataStore::account_from_row).await;
        Ok(list)
    }

    pub async fn get_balance(db: Arc<Database>, account: AccountRef) -> Result<Money<'static, Currency>, String> {
        let rslt = db
            .query_opt(
                "SELECT data FROM bank_accounts WHERE number = $1",
                &[&account.number],
                DataStore::account_from_row,
            )
            .await;

        match rslt {
            Some(account) => Ok(account.balance),
            None => Err(format!("There is no bank account with number: {}", account.number)),
        }
    }

    pub async fn deposit(db: Arc<Database>, account_ref: AccountRef, amount: String) -> Result<(), String> {
        if let Some(new_version) = account_ref.new_version {
            let sql = r#"
                UPDATE bank_accounts SET data = data ||
                jsonb_build_object(
                    'amount', ((data->>'amount')::DECIMAL + (($1)::TEXT)::DECIMAL)::TEXT,
                    'talosState', jsonb_build_object('version', ($2)::BIGINT)
                )
                WHERE "number" = $3
                RETURNING data
            "#;
            db.query_one(sql, &[&amount, &(new_version as i64), &account_ref.number], DataStore::account_from_row)
                .await;
        } else {
            let sql = r#"
                UPDATE bank_accounts SET data = data ||
                jsonb_build_object(
                    'amount', ((data->>'amount')::DECIMAL + (($1)::TEXT)::DECIMAL)::TEXT
                )
                WHERE "number" = $2
                RETURNING data
            "#;
            db.query_one(sql, &[&amount, &account_ref.number], DataStore::account_from_row).await;
        }

        Ok(())
    }

    pub async fn withdraw(db: Arc<Database>, account_ref: AccountRef, amount: String) -> Result<(), String> {
        Self::deposit(db, account_ref, format!("-{}", amount)).await
    }

    pub async fn transfer(db: Arc<Database>, from: AccountRef, to: AccountRef, amount: String) -> Result<(), String> {
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
}
