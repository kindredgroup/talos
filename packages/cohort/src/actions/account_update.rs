use async_trait::async_trait;
use std::fmt;
use std::fmt::{Display, Formatter};

use deadpool_postgres::GenericClient;
use tokio_postgres::types::ToSql;

use crate::state::{model::AccountUpdateRequest, postgres::database::Action};

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
        r#"
            UPDATE bank_accounts SET data = data ||
            jsonb_build_object(
                'amount', ((data->>'amount')::DECIMAL + (($1)::TEXT)::DECIMAL)::TEXT,
                'talosState', jsonb_build_object('version', ($2)::BIGINT)
            )
            WHERE "number" = $3 AND (data->'talosState'->'version')::BIGINT <= ($2)::BIGINT
        "#
    }
}

#[async_trait]
impl Action for AccountUpdate {
    async fn execute<T>(&self, client: &T) -> Result<u64, String>
    where
        T: GenericClient + Sync,
    {
        let params: &[&(dyn ToSql + Sync)] = &[&self.data.amount, &(self.new_version as i64), &self.data.account];

        let statement = client.prepare_cached(Self::sql()).await.unwrap();
        client.execute(&statement, params).await.map_err(|e| e.to_string())
    }
}
