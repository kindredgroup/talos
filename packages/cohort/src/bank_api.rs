use async_trait::async_trait;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use deadpool_postgres::GenericClient;
use rusty_money::iso::Currency;
use rusty_money::Money;
use tokio_postgres::types::ToSql;

use crate::model::bank_account::BankAccount;
use crate::state::model::AccountRef;
use crate::state::postgres::data_store::DataStore;
use crate::state::postgres::database::{Action, Database};

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

    pub async fn deposit(db: Arc<Database>, account_ref: AccountRef, amount: String, new_version: u64) -> Result<u64, String> {
        AccountUpdate::deposit(account_ref, amount, new_version, true)
            .execute(&db.pool.get().await.unwrap())
            .await
    }

    pub async fn withdraw(db: Arc<Database>, account_ref: AccountRef, amount: String, new_version: u64) -> Result<u64, String> {
        Self::deposit(db, account_ref, format!("-{}", amount), new_version).await
    }

    pub async fn transfer(db: Arc<Database>, from: AccountRef, to: AccountRef, amount: String, new_version: u64) -> Result<u64, String> {
        let affected_rows = Self::transfer_one(db, Transfer::new(from.clone(), to.clone(), amount.clone(), new_version, true)).await?;
        if affected_rows != 2 {
            Err(format!(
                "Unable to transfer ${} from '{}' to '{}'. Error: affected rows({}) != 2",
                amount, from, to, affected_rows,
            ))
        } else {
            Ok(affected_rows)
        }
    }

    pub async fn transfer_one(db: Arc<Database>, action: Transfer) -> Result<u64, String> {
        action.execute(&db.pool.get().await.unwrap()).await
    }
}

#[derive(Debug)]
pub struct AccountUpdate {
    pub account: AccountRef,
    pub amount: String,
    pub action: &'static str,
    pub new_version: u64,
    pub update_version: bool,
}

impl Display for AccountUpdate {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AccountUpdate: [action: {}, account: {}, amount: {}]",
            self.action, self.account, self.amount,
        )
    }
}

impl AccountUpdate {
    pub fn deposit(account: AccountRef, amount: String, new_version: u64, update_version: bool) -> Self {
        Self {
            account,
            amount: amount.replace(['-', '+'], ""),
            action: "Deposit",
            new_version,
            update_version,
        }
    }

    pub fn withdraw(account: AccountRef, amount: String, new_version: u64, update_version: bool) -> Self {
        Self {
            account,
            amount: format!("-{}", amount.replace(['-', '+'], "")),
            action: "Withdraw",
            new_version,
            update_version,
        }
    }

    fn sql(update_version: bool) -> &'static str {
        if update_version {
            r#"
            UPDATE bank_accounts SET data = data ||
            jsonb_build_object(
                'amount', ((data->>'amount')::DECIMAL + (($1)::TEXT)::DECIMAL)::TEXT,
                'talosState', jsonb_build_object('version', ($2)::BIGINT)
            )
            WHERE "number" = $3 AND (data->'talosState'->'version')::BIGINT < ($2)::BIGINT
        "#
        } else {
            r#"
            UPDATE bank_accounts SET data = data ||
            jsonb_build_object('amount', ((data->>'amount')::DECIMAL + (($1)::TEXT)::DECIMAL)::TEXT)
            WHERE "number" = $3 AND (data->'talosState'->'version')::BIGINT < ($2)::BIGINT
        "#
        }
    }
}

#[async_trait]
impl Action for AccountUpdate {
    async fn execute<T>(&self, client: &T) -> Result<u64, String>
    where
        T: GenericClient + Sync,
    {
        let params: &[&(dyn ToSql + Sync)] = &[&self.amount, &(self.new_version as i64), &self.account.number];

        let statement = client.prepare_cached(Self::sql(self.update_version)).await.unwrap();
        client.execute(&statement, params).await.map_err(|e| e.to_string())
    }
}

#[derive(Debug)]
pub struct Transfer {
    pub from: AccountRef,
    pub to: AccountRef,
    pub amount: String,
    pub new_version: u64,
    pub update_version: bool,
}

impl Display for Transfer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Transfer: [from: {}, to: {}, amount: {}]", self.from, self.to, self.amount)
    }
}

impl Transfer {
    pub fn new(from: AccountRef, to: AccountRef, amount: String, new_version: u64, update_version: bool) -> Transfer {
        Transfer {
            from,
            to,
            amount,
            new_version,
            update_version,
        }
    }

    fn sql(update_version: bool) -> &'static str {
        if update_version {
            r#"
            UPDATE bank_accounts SET data = COALESCE(
                CASE
                    WHEN "number" = ($2)::TEXT AND (data->'talosState'->>'version')::BIGINT < ($3)::BIGINT THEN data || jsonb_build_object('amount', ((data->>'amount')::DECIMAL - (($1)::TEXT)::DECIMAL)::TEXT, 'talosState', jsonb_build_object('version', ($3)::BIGINT))
                    WHEN "number" = ($4)::TEXT AND (data->'talosState'->>'version')::BIGINT < ($5)::BIGINT THEN data || jsonb_build_object('amount', ((data->>'amount')::DECIMAL + (($1)::TEXT)::DECIMAL)::TEXT, 'talosState', jsonb_build_object('version', ($5)::BIGINT))
                END, data)
            WHERE "number" in(($2)::TEXT, ($4)::TEXT)
        "#
        } else {
            r#"
            UPDATE bank_accounts SET data = COALESCE(
                CASE
                    WHEN "number" = ($2)::TEXT AND (data->'talosState'->>'version')::BIGINT < ($3)::BIGINT THEN data || jsonb_build_object('amount', ((data->>'amount')::DECIMAL - (($1)::TEXT)::DECIMAL)::TEXT)
                    WHEN "number" = ($4)::TEXT AND (data->'talosState'->>'version')::BIGINT < ($5)::BIGINT THEN data || jsonb_build_object('amount', ((data->>'amount')::DECIMAL + (($1)::TEXT)::DECIMAL)::TEXT)
                END, data)
            WHERE "number" in(($2)::TEXT, ($4)::TEXT)
        "#
        }
    }
}

#[async_trait]
impl Action for Transfer {
    async fn execute<T>(&self, client: &T) -> Result<u64, String>
    where
        T: GenericClient + Sync,
    {
        let params: &[&(dyn ToSql + Sync)] = &[
            &self.amount,
            &self.from.number,
            &(self.new_version as i64),
            &self.to.number,
            &(self.new_version as i64),
        ];

        let statement = client.prepare_cached(Self::sql(self.update_version)).await.unwrap();
        client.execute(&statement, params).await.map_err(|e| e.to_string())
    }
}
