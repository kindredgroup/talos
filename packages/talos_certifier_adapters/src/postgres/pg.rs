use std::time::Duration;

use async_trait::async_trait;
use deadpool_postgres::{Config, ManagerConfig, Object, Pool, PoolConfig, PoolError, Runtime};
use futures_util::{pin_mut, TryStreamExt};
use itertools::Itertools;
use log::{error, warn};
use serde_json::{json, Value};
use talos_certifier::{
    model::DecisionMessage,
    ports::{
        common::SharedPortTraits,
        errors::{DecisionStoreError, DecisionStoreErrorKind},
        DecisionStore,
    },
};
use tokio_postgres::{types::ToSql, NoTls, Statement};

use crate::{PgConfig, PgError};

use super::utils::{get_uuid_key, parse_json_column};

#[derive(Clone)]
pub struct Pg {
    pub pool: Pool,
}
// create postgres client
impl Pg {
    pub async fn new(pg_config: PgConfig) -> Result<Self, PgError> {
        let mut config = Config::new();
        config.dbname = Some(pg_config.database);
        config.user = Some(pg_config.user);
        config.password = Some(pg_config.password);
        config.host = Some(pg_config.host);
        config.port = Some(pg_config.port.parse::<u16>().expect("Failed to parse port to u16"));
        config.manager = Some(ManagerConfig {
            recycling_method: deadpool_postgres::RecyclingMethod::Fast,
        });
        config.pool = Some(PoolConfig::new(400));

        let pool = config.create_pool(Some(Runtime::Tokio1), NoTls).map_err(PgError::CreatePool)?;

        //test connection
        let _ = pool.get().await.map_err(PgError::GetClientFromPool)?;

        Ok(Pg { pool })
    }

    pub async fn get_client(&self) -> Result<Object, PgError> {
        let client = self.pool.get().await.map_err(PgError::GetClientFromPool)?;

        Ok(client)
    }

    pub async fn get_client_with_retry(&self) -> Result<Object, PgError> {
        let mut interval = tokio::time::interval(Duration::from_millis(5_000));

        loop {
            let result = self.pool.get().await;

            match result {
                Ok(pool_object) => return Ok(pool_object),
                Err(pool_error) => match pool_error {
                    PoolError::Backend(_) | PoolError::Timeout(_) => {
                        interval.tick().await;
                        warn!("Error retreiving pool object, retrying...");
                        continue;
                    }
                    _ => return Err(PgError::GetClientFromPool(pool_error)),
                },
            };
        }
    }
}

#[async_trait]
impl DecisionStore for Pg {
    type Decision = DecisionMessage;

    async fn get_decision(&self, key: String) -> Result<Option<Self::Decision>, DecisionStoreError> {
        let client = self.get_client_with_retry().await.map_err(|e| DecisionStoreError {
            kind: DecisionStoreErrorKind::ClientError,
            reason: e.to_string(),
            data: None,
        })?;
        let stmt = client.prepare_cached("SELECT xid, decision from xdb where xid = $1").await.unwrap();

        let key_uuid = get_uuid_key(&key)?;
        let rows = client.query_opt(&stmt, &[&key_uuid]).await.map_err(|e| DecisionStoreError {
            kind: DecisionStoreErrorKind::GetDecision,
            reason: e.to_string(),
            data: Some(key.clone()),
        })?;

        let Some(row) = rows else {
            return Ok(None);
        };

        let val = row.get::<&str, Option<Value>>("decision");
        let Some(value) = val else {
            return Ok(None);
        };

        Ok(Some(parse_json_column(&key, value)?))
    }

    async fn insert_decision(&self, key: String, decision: Self::Decision) -> Result<Self::Decision, DecisionStoreError> {
        let client = self.get_client_with_retry().await.map_err(|e| DecisionStoreError {
            kind: DecisionStoreErrorKind::ClientError,
            reason: e.to_string(),
            data: None,
        })?;

        let key_uuid = get_uuid_key(&key)?;

        // let stmt = client
        //     .prepare_cached(
        //         "WITH ins AS (
        //             INSERT INTO xdb(xid, decision)
        //             VALUES ($1, $2)
        //             ON CONFLICT DO NOTHING
        //             RETURNING xid, decision
        //         )
        //         SELECT * from ins
        //         UNION
        //         SELECT xid, decision from xdb where xid = $1",
        //     )
        //     .await
        //     .map_err(|e| DecisionStoreError {
        //         kind: DecisionStoreErrorKind::InsertDecision,
        //         reason: e.to_string(),
        //         data: Some(key.clone()),
        //     })?;
        let stmt = client
            .prepare_cached(
                "
                    INSERT INTO xdb(xid, decision)
                    VALUES ($1, $2)
                    ON CONFLICT DO NOTHING
                    RETURNING xid, decision
                ",
            )
            .await
            .map_err(|e| DecisionStoreError {
                kind: DecisionStoreErrorKind::InsertDecision,
                reason: e.to_string(),
                data: Some(key.clone()),
            })?;

        // Execute insert returning the row. If duplicate is found, return the existing row in table.
        let result = client.query_one(&stmt, &[&key_uuid, &json!(decision)]).await;
        match result {
            Ok(row) => {
                let decision = match row.get::<&str, Option<Value>>("decision") {
                    Some(value) => Ok(parse_json_column(&key, value)?),
                    None => match self.get_decision(key.clone()).await? {
                        Some(v) => Ok(v),
                        None => Err(DecisionStoreError {
                            kind: DecisionStoreErrorKind::NoRowReturned,
                            reason: "Insert did not return rows".to_owned(),
                            data: Some(key.clone()),
                        }),
                    },
                    // _ => Err(DecisionStoreError {
                    //     kind: DecisionStoreErrorKind::NoRowReturned,
                    //     reason: "Insert did not return rows".to_owned(),
                    //     data: Some(key.clone()),
                    // }),
                };

                return decision;
            }
            Err(e) => {
                return Err(DecisionStoreError {
                    kind: DecisionStoreErrorKind::InsertDecision,
                    reason: e.to_string(),
                    data: Some(key.clone()),
                });
            }
        }

        // Ok(())
    }
    async fn insert_decision_multi(&self, decisions: Vec<Self::Decision>) -> Result<Vec<Self::Decision>, DecisionStoreError> {
        let client = self.get_client_with_retry().await.map_err(|e| DecisionStoreError {
            kind: DecisionStoreErrorKind::ClientError,
            reason: e.to_string(),
            data: None,
        })?;

        // let decisions_1 = decisions.iter().map(|d| (d.xid.clone(), json!(d))).collect::<Vec<(String, Value)>>();
        let decisions_1 = decisions
            .iter()
            .map(|d| format!("('{}','{}')", d.xid.clone(), json!(d)))
            .collect::<Vec<String>>();

        // let params: Vec<_> = decisions
        //     .iter()
        //     .flat_map(|row| {
        //         let xid = &row.xid;
        //         let d = &json!(row);
        //         [xid as &(dyn ToSql + Sync), &d]
        //     })
        //     .collect();
        let format_str = format!(
            "INSERT INTO xdb(xid, decision) VALUES {} ON CONFLICT DO NOTHING RETURNING xid, decision",
            // decisions_1.iter().format_with(", ", |(i, j), f| { f(&format_args!("(${i}, ${j})")) }),
            decisions_1.join(", "),
        );

        // error!("formatted string is \n {:#?}", format_str);
        // format

        // let key_uuid = get_uuid_key(&key)?;

        // let stmt = client
        //     .prepare_cached(
        //         "WITH ins AS (
        //             INSERT INTO xdb(xid, decision)
        //             VALUES ($1, $2)
        //             ON CONFLICT DO NOTHING
        //             RETURNING xid, decision
        //         )
        //         SELECT * from ins
        //         UNION
        //         SELECT xid, decision from xdb where xid = $1",
        //     )
        //     .await
        //     .map_err(|e| DecisionStoreError {
        //         kind: DecisionStoreErrorKind::InsertDecision,
        //         reason: e.to_string(),
        //         data: Some(key.clone()),
        //     })?;
        let stmt = client.prepare_cached(&format_str).await.map_err(|e| DecisionStoreError {
            kind: DecisionStoreErrorKind::InsertDecision,
            reason: e.to_string(),
            // data: Some(key.clone()),
            data: None,
        })?;

        // error!("Statement CREATED!!!");

        // Execute insert returning the row. If duplicate is found, return the existing row in table.
        let result = client.query(&stmt, &[]).await;
        match result {
            Ok(row) => {
                // let decision = match row.get::<&str, Option<Value>>("decision") {
                //     Some(value) => Ok(parse_json_column(&key, value)?),
                //     None => match self.get_decision(key.clone()).await? {
                //         Some(v) => Ok(v),
                //         None => Err(DecisionStoreError {
                //             kind: DecisionStoreErrorKind::NoRowReturned,
                //             reason: "Insert did not return rows".to_owned(),
                //             data: Some(key.clone()),
                //         }),
                //     },
                //     // _ => Err(DecisionStoreError {
                //     //     kind: DecisionStoreErrorKind::NoRowReturned,
                //     //     reason: "Insert did not return rows".to_owned(),
                //     //     data: Some(key.clone()),
                //     // }),
                // };
                // pin_mut!(row);
                // let decision = match row.try_next().await.unwrap().unwrap().get("decision") {
                //     Some(value) => {
                //         let parsed_value = parse_json_column("", value);
                //         error!("Parsed value is \n {:#?}", parsed_value);
                //         Ok(vec![parsed_value?])
                //     }
                //     // None => match self.get_decision(key.clone()).await? {
                //     //     Some(v) => Ok(vec![v]),
                //     //     None => Err(DecisionStoreError {
                //     //         kind: DecisionStoreErrorKind::NoRowReturned,
                //     //         reason: "Insert did not return rows".to_owned(),
                //     //         // data: Some(key.clone()),
                //     //         data: None,
                //     //     }),
                //     // },
                //     _ => Err(DecisionStoreError {
                //         kind: DecisionStoreErrorKind::NoRowReturned,
                //         reason: "Insert did not return rows".to_owned(),
                //         // data: Some(key.clone()),
                //         data: None,
                //     }),
                // };

                // return decision;

                let mut decisions: Vec<DecisionMessage> = vec![];

                for r in row {
                    let d = r.get::<&str, Option<Value>>("decision");

                    match d {
                        Some(k) => {
                            let dd1 = serde_json::from_value::<DecisionMessage>(k).unwrap();
                            decisions.push(dd1)
                        }
                        None => todo!(),
                    };
                }

                return Ok(decisions);
            }
            Err(e) => {
                error!("Error inserting... {}", e);
                return Err(DecisionStoreError {
                    kind: DecisionStoreErrorKind::InsertDecision,
                    reason: e.to_string(),
                    // data: Some(key.clone()),
                    data: None,
                });
            }
        }

        // Ok(())
    }
}

#[async_trait]
impl SharedPortTraits for Pg {
    async fn is_healthy(&self) -> bool {
        //TODO - Implement health check here
        true
    }

    async fn shutdown(&self) -> bool {
        self.pool.close();

        self.pool.is_closed()
    }
}
