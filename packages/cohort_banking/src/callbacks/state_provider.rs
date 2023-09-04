use std::sync::Arc;

use cohort_sdk::model::{CertificationCandidate, CertificationCandidateCallbackResponse, CertificationRequestPayload};
use rust_decimal::Decimal;
use tokio_postgres::Row;

use crate::{
    model::{bank_account::BankAccount, requests::CertificationRequest},
    state::postgres::database::{Database, DatabaseError},
};

#[derive(Debug, PartialEq, PartialOrd)]
pub struct CapturedState {
    pub snapshot_version: u64,
    pub items: Vec<CapturedItemState>,
}

#[derive(Debug, PartialEq, PartialOrd)]
pub struct CapturedItemState {
    pub id: String,
    pub version: u64,
}

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
    async fn get_state_using_two_queries(&self, from_account: &str, to_account: &str) -> Result<CapturedState, String> {
        let list = self
            .database
            .query_many(
                r#"SELECT ba.* FROM bank_accounts ba WHERE ba."number" = $1 OR ba."number" = $2"#,
                &[&from_account, &to_account],
                |row| BankAccount::try_from(row),
            )
            .await
            .map_err(|e| e.to_string())?;

        if list.len() != 2 {
            return Err(format!("Unable to load state of accounts: '{:?}' and '{:?}'", from_account, to_account));
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

    async fn get_state_using_one_query(&self, from_account: &str, to_account: &str) -> Result<CapturedState, String> {
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
                &[&from_account, &to_account],
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
            return Err(format!("Unable to load state of accounts: '{:?}' and '{:?}'", from_account, to_account));
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

        // The order of the accounts doesn't really matter in this example as we just use these accounts to get their respective versions.
        // Safe assumption made here that we have 2 items in the writeset here. As our example is to transfer between 2 accounts.
        // The other alternative is to deserialize from statemap Value, which could be expensive comparitively, also we may not have statemap.
        let first_account = &request.candidate.writeset[0];
        let second_account = &request.candidate.writeset[1];

        let state = if self.single_query_strategy {
            self.get_state_using_one_query(first_account, second_account).await
        } else {
            self.get_state_using_two_queries(first_account, second_account).await
        }?;

        let candidate = CertificationCandidate {
            readset: request.candidate.readset,
            writeset: request.candidate.writeset,
            statemaps: request.candidate.statemap,
            readvers: state.items.into_iter().map(|x| x.version).collect(),
        };

        Ok(CertificationCandidateCallbackResponse::Proceed(CertificationRequestPayload {
            candidate,
            snapshot: state.snapshot_version,
            timeout_ms: request.timeout_ms,
        }))
    }
}
