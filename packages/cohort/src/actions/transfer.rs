use std::fmt;
use std::fmt::{Display, Formatter};

use async_trait::async_trait;
use deadpool_postgres::GenericClient;
use tokio_postgres::types::ToSql;

use crate::state::postgres::database::Action;

#[derive(Debug)]
pub struct Transfer {
    pub from: String,
    pub to: String,
    pub amount: String,
    pub new_version: u64,
}

impl Display for Transfer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Transfer: [from: {}, to: {}, amount: {}]", self.from, self.to, self.amount)
    }
}

impl Transfer {
    pub fn new(from: String, to: String, amount: String, new_version: u64) -> Transfer {
        Transfer { from, to, amount, new_version }
    }

    fn sql() -> &'static str {
        r#"
        UPDATE bank_accounts SET data = COALESCE(
            CASE
                WHEN "number" = ($2)::TEXT AND (data->'talosState'->>'version')::BIGINT <= ($3)::BIGINT THEN data || jsonb_build_object('amount', ((data->>'amount')::DECIMAL - (($1)::TEXT)::DECIMAL)::TEXT, 'talosState', jsonb_build_object('version', ($3)::BIGINT))
                WHEN "number" = ($4)::TEXT AND (data->'talosState'->>'version')::BIGINT <= ($5)::BIGINT THEN data || jsonb_build_object('amount', ((data->>'amount')::DECIMAL + (($1)::TEXT)::DECIMAL)::TEXT, 'talosState', jsonb_build_object('version', ($5)::BIGINT))
            END, data)
        WHERE "number" in(($2)::TEXT, ($4)::TEXT)
        "#
    }
}

#[async_trait]
impl Action for Transfer {
    async fn execute<T>(&self, client: &T) -> Result<u64, String>
    where
        T: GenericClient + Sync,
    {
        let params: &[&(dyn ToSql + Sync)] = &[&self.amount, &self.from, &(self.new_version as i64), &self.to, &(self.new_version as i64)];

        let statement = client.prepare_cached(Self::sql()).await.unwrap();
        client.execute(&statement, params).await.map_err(|e| e.to_string())
    }
}
