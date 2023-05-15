use async_trait::async_trait;
use std::fmt;
use std::fmt::{Display, Formatter};

use tokio_postgres::types::ToSql;

use crate::model::requests::AccountUpdateRequest;
use crate::state::data_access_api::{Connection, ManualTx};

use super::action::Action;

pub static ACCOUNT_UPDATE_QUERY: &str = r#"UPDATE bank_accounts SET data = data ||
        jsonb_build_object(
            'amount', ((data->>'amount')::DECIMAL + (($1)::TEXT)::DECIMAL)::TEXT
        )
        WHERE "number" = $3 AND (data->'talosState'->'version')::BIGINT < ($2)::BIGINT"#;

pub static ACCOUNT_VERSION_UPDATE_QUERY: &str = r#"UPDATE bank_accounts SET data = data ||
        jsonb_build_object(
            'talosState', jsonb_build_object('version', ($2)::BIGINT)
        )
        WHERE "number" = $1 AND (data->'talosState'->'version')::BIGINT < ($2)::BIGINT"#;

#[derive(Debug)]
pub struct AccountUpdate {
    pub data: AccountUpdateRequest,
    pub action: &'static str,
    pub new_version: u64,
}

impl Display for AccountUpdate {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AccountUpdate: [action: {}, account: {}, amount: {}]",
            self.action, self.data.account, self.data.amount,
        )
    }
}

impl AccountUpdate {
    pub fn deposit(data: AccountUpdateRequest, new_version: u64) -> Self {
        Self {
            data: AccountUpdateRequest::new(data.account, data.amount.replace(['-', '+'], "")),
            action: "Deposit",
            new_version,
        }
    }

    pub fn withdraw(data: AccountUpdateRequest, new_version: u64) -> Self {
        Self {
            data: AccountUpdateRequest::new(data.account, format!("-{}", data.amount.replace(['-', '+'], ""))),
            action: "Withdraw",
            new_version,
        }
    }

    fn sql() -> &'static str {
        ACCOUNT_UPDATE_QUERY
    }

    fn sql_update_version() -> &'static str {
        ACCOUNT_VERSION_UPDATE_QUERY
    }
}

#[async_trait]
impl Action for AccountUpdate {
    async fn execute<T: ManualTx>(&self, client: &T) -> Result<u64, String> {
        let params: &[&(dyn ToSql + Sync)] = &[&self.data.amount, &(self.new_version as i64), &self.data.account];

        client.execute(Self::sql().to_string(), params).await
    }

    async fn update_version<T: ManualTx>(&self, client: &T) -> Result<u64, String> {
        let params: &[&(dyn ToSql + Sync)] = &[&self.data.account, &(self.new_version as i64)];

        client.execute(Self::sql_update_version().to_string(), params).await
    }

    async fn execute_in_db<T: Connection>(&self, client: &T) -> Result<u64, String> {
        let params: &[&(dyn ToSql + Sync)] = &[&self.data.amount, &(self.new_version as i64), &self.data.account];

        client.execute(Self::sql().to_string(), params).await
    }
}
