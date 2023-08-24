use std::sync::Arc;

use cohort_sdk::model::{
    callbacks::{CapturedItemState, CapturedState, CertificationCandidateCallbackResponse},
    CertificationCandidate, CertificationRequestPayload,
};
use rust_decimal::Decimal;
use tokio_postgres::{types::ToSql, Row};

use crate::{
    model::{bank_account::BankAccount, requests::CertificationRequest},
    state::postgres::database::{Database, DatabaseError},
};

impl TryFrom<&Row> for BankAccount {
    type Error = DatabaseError;

    fn try_from(row: &Row) -> Result<Self, Self::Error> {
        Ok(BankAccount {
            name: row
                .try_get::<&str, String>("name")
                .map_err(|e| DatabaseError::deserialise_payload(e.to_string(), "Cannot read account name".into()))?,
            number: row
                .try_get::<&str, String>("number")
                .map_err(|e| DatabaseError::deserialise_payload(e.to_string(), "Cannot read account number".into()))?,
            version: row
                .try_get::<&str, i64>("version")
                .map_err(|e| DatabaseError::deserialise_payload(e.to_string(), "Cannot read account version".into()))? as u64,
            balance: row
                .try_get::<&str, Decimal>("amount")
                .map_err(|e| DatabaseError::deserialise_payload(e.to_string(), "Cannot read account amount".into()))?,
        })
    }
}

pub struct StateProviderImpl {
    pub database: Arc<Database>,
    pub single_query_strategy: bool,
}

impl StateProviderImpl {
    async fn get_state_using_two_queries(&self, accounts: &[&(dyn ToSql + Sync)]) -> Result<CapturedState, String> {
        let list = self
            .database
            .query_many(
                r#"SELECT ba.* FROM bank_accounts ba WHERE ba."number" = $1 OR ba."number" = $2"#,
                accounts,
                |row| BankAccount::try_from(row),
            )
            .await
            .map_err(|e| e.to_string())?;

        if list.len() != 2 {
            return Err(format!("Unable to load state of accounts: '{:?}' and '{:?}'", accounts[0], accounts[1]));
        }

        let snapshot_version = self
            .database
            .query_one(
                r#"SELECT cs."version" AS snapshot_version FROM cohort_snapshot cs WHERE cs.id = $1"#,
                &[&"SINGLETON"],
                |row| {
                    let snapshot_version = row
                        .try_get::<&str, i64>("snapshot_version")
                        .map_err(|e| DatabaseError::deserialise_payload(e.to_string(), "Cannot read snapshot_version".into()))?;
                    Ok(snapshot_version as u64)
                },
            )
            .await
            .map_err(|e| e.to_string())?;

        Ok(CapturedState {
            snapshot_version,
            items: list
                .iter()
                .map(|account| CapturedItemState {
                    id: account.number.clone(),
                    version: account.version,
                })
                .collect(),
        })
    }

    async fn get_state_using_one_query(&self, accounts: &[&(dyn ToSql + Sync)]) -> Result<CapturedState, String> {
        let list = self
            .database
            .query_many(
                // Note:
                // We intentioanly left 'cohort_snapshot' table not joined to 'bank_accounts', in that case
                // database will multiply its content with 'bank_accounts' and automatically join rows from both tables.
                // Given that 'cohort_snapshot' will ever have one single row, we are good here.
                r#"
                SELECT
                    ba.*, cs."version" AS snapshot_version
                FROM
                    bank_accounts ba, cohort_snapshot cs
                WHERE
                    ba."number" = $1 OR ba."number" = $2"#,
                accounts,
                // convert RAW output into tuple (bank account, snap ver)
                |row| {
                    let account = BankAccount::try_from(row)?;
                    let snapshot_version = row
                        .try_get::<&str, i64>("snapshot_version")
                        .map_err(|e| DatabaseError::deserialise_payload(e.to_string(), "Cannot read snapshot_version".into()))?;
                    Ok((account, snapshot_version as u64))
                },
            )
            .await
            .map_err(|e| e.to_string())?;

        if list.len() != 2 {
            return Err(format!("Unable to load state of accounts: '{:?}' and '{:?}'", accounts[0], accounts[1]));
        }

        Ok(CapturedState {
            snapshot_version: list[0].1,
            items: list
                .iter()
                .map(|tuple| CapturedItemState {
                    id: tuple.0.number.clone(),
                    version: tuple.0.version,
                })
                .collect(),
        })
    }

    pub async fn get_certification_candidate(&self, request: CertificationRequest) -> Result<CertificationCandidateCallbackResponse, String> {
        // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        // This example doesn't handle `Cancelled` scenario.
        // If user cancellation is needed, add additional logic in this fn to return `Cancelled` instead of `Proceed` in the result.
        // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

        let mut accounts: Vec<&(dyn ToSql + Sync)> = vec![];
        request.candidate.readset.iter().for_each(|x| accounts.push(x));

        let state = if self.single_query_strategy {
            self.get_state_using_one_query(&accounts).await
        } else {
            self.get_state_using_two_queries(&accounts).await
        }?;

        let candidate = CertificationCandidate {
            readset: request.candidate.readset,
            writeset: request.candidate.writeset,
            statemap: request.candidate.statemap,
            readvers: state.items.into_iter().map(|x| x.version).collect(),
        };

        Ok(CertificationCandidateCallbackResponse::Proceed(CertificationRequestPayload {
            candidate,
            snapshot: state.snapshot_version,
            timeout_ms: request.timeout_ms,
        }))
    }
}
