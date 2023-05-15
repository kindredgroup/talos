use std::fmt;
use std::fmt::{Display, Formatter};

use async_trait::async_trait;
use tokio_postgres::types::ToSql;

use crate::model::requests::TransferRequest;
use crate::state::data_access_api::{Connection, ManualTx};

use super::action::Action;

#[derive(Debug)]
pub struct Transfer {
    pub data: TransferRequest,
    pub new_version: u64,
}

impl Display for Transfer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Transfer: [from: {}, to: {}, amount: {}]", self.data.from, self.data.to, self.data.amount)
    }
}

impl Transfer {
    pub fn new(data: TransferRequest, new_version: u64) -> Self {
        Self { data, new_version }
    }

    fn sql() -> &'static str {
        r#"
        UPDATE bank_accounts SET data = COALESCE(
            CASE
                WHEN "number" = ($2)::TEXT AND (data->'talosState'->>'version')::BIGINT < ($3)::BIGINT THEN data || jsonb_build_object('amount', ((data->>'amount')::DECIMAL - (($1)::TEXT)::DECIMAL)::TEXT)
                WHEN "number" = ($4)::TEXT AND (data->'talosState'->>'version')::BIGINT < ($5)::BIGINT THEN data || jsonb_build_object('amount', ((data->>'amount')::DECIMAL + (($1)::TEXT)::DECIMAL)::TEXT)
            END, data)
        WHERE "number" in(($2)::TEXT, ($4)::TEXT)
        "#
    }

    fn sql_update_version() -> &'static str {
        r#"
        UPDATE bank_accounts SET data = COALESCE(
            CASE
                WHEN "number" = ($1)::TEXT AND (data->'talosState'->>'version')::BIGINT < ($2)::BIGINT THEN data || jsonb_build_object('talosState', jsonb_build_object('version', ($2)::BIGINT))
                WHEN "number" = ($3)::TEXT AND (data->'talosState'->>'version')::BIGINT < ($2)::BIGINT THEN data || jsonb_build_object('talosState', jsonb_build_object('version', ($2)::BIGINT))
            END, data)
        WHERE "number" in(($1)::TEXT, ($3)::TEXT)
        "#
    }
}

#[async_trait]
impl Action for Transfer {
    async fn execute<T: ManualTx>(&self, client: &T) -> Result<u64, String> {
        let params: &[&(dyn ToSql + Sync)] = &[
            &self.data.amount,
            &self.data.from,
            &(self.new_version as i64),
            &self.data.to,
            &(self.new_version as i64),
        ];

        client.execute(Self::sql().to_string(), params).await
    }

    async fn update_version<T: ManualTx>(&self, client: &T) -> Result<u64, String> {
        let params: &[&(dyn ToSql + Sync)] = &[&self.data.from, &(self.new_version as i64), &self.data.to];

        client.execute(Self::sql_update_version().to_string(), params).await
    }

    async fn execute_in_db<T: Connection>(&self, client: &T) -> Result<u64, String> {
        let params: &[&(dyn ToSql + Sync)] = &[
            &self.data.amount,
            &self.data.from,
            &(self.new_version as i64),
            &self.data.to,
            &(self.new_version as i64),
        ];

        client.execute(Self::sql().to_string(), params).await
    }
}
