// $coverage:ignore-start
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rust_decimal::Decimal;
use talos_agent::agent::core::TalosAgentImpl;
use talos_agent::agent::model::{CancelRequestChannelMessage, CertifyRequestChannelMessage};
use talos_agent::api::{AgentConfig, CandidateData, CertificationRequest, CertificationResponse, KafkaConfig, TalosAgent};
use talos_agent::messaging::api::{Decision, DecisionMessage};
use talos_agent::messaging::kafka::KafkaInitializer;
use talos_agent::metrics::client::MetricsClient;
use talos_agent::metrics::core::Metrics;
use talos_agent::metrics::model::Signal;
use talos_agent::mpsc::core::{ReceiverWrapper, SenderWrapper};
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tracing::instrument;

use crate::bank_api::BankApi;

use crate::metrics::{Span, TxExecSpans};
use crate::model::bank_account::BankAccount;
use crate::model::requests::{BusinessActionType, TransferRequest};

use crate::model::snapshot::Snapshot;
use crate::model::tx_execution::BankResult;
use crate::snapshot_api::SnapshotApi;
use crate::state::postgres::database::Database;

pub type AgentType = Box<dyn TalosAgent + Sync + Send>;

pub struct Cohort {}

impl Cohort {
    pub async fn init_agent(config: AgentConfig, kafka_config: KafkaConfig) -> AgentType {
        let (tx_certify_ch, rx_certify_ch) = tokio::sync::mpsc::channel::<CertifyRequestChannelMessage>(config.buffer_size);
        let tx_certify = SenderWrapper::<CertifyRequestChannelMessage> { tx: tx_certify_ch };
        let rx_certify = ReceiverWrapper::<CertifyRequestChannelMessage> { rx: rx_certify_ch };

        let (tx_decision_ch, rx_decision_ch) = tokio::sync::mpsc::channel::<DecisionMessage>(config.buffer_size);
        let tx_decision = SenderWrapper::<DecisionMessage> { tx: tx_decision_ch };
        let rx_decision = ReceiverWrapper::<DecisionMessage> { rx: rx_decision_ch };

        let (tx_cancel_ch, rx_cancel_ch) = tokio::sync::mpsc::channel::<CancelRequestChannelMessage>(config.buffer_size);
        let tx_cancel = SenderWrapper::<CancelRequestChannelMessage> { tx: tx_cancel_ch };
        let rx_cancel = ReceiverWrapper::<CancelRequestChannelMessage> { rx: rx_cancel_ch };

        let (publisher, consumer) = KafkaInitializer::connect(config.agent.clone(), kafka_config)
            .await
            .expect("Cannot connect to kafka...");

        let server = Metrics::new();

        let (tx_ch, rx_ch) = mpsc::channel::<Signal>(10_000_000);
        server.run(ReceiverWrapper { rx: rx_ch });

        let client = MetricsClient {
            tx_destination: SenderWrapper::<Signal> { tx: tx_ch },
        };

        let metrics_client = Some(Box::new(client));
        let metrics = Some(server);

        let agent = TalosAgentImpl::new(
            config.clone(),
            Arc::new(Box::new(tx_certify)),
            tx_cancel,
            metrics,
            Arc::new(metrics_client),
            || {
                let (tx_ch, rx_ch) = tokio::sync::mpsc::channel::<CertificationResponse>(1);
                (SenderWrapper { tx: tx_ch }, ReceiverWrapper { rx: rx_ch })
            },
        );

        agent
            .start(rx_certify, rx_cancel, tx_decision, rx_decision, publisher, consumer)
            .expect("unable to start agent");

        Box::new(agent)
    }

    #[instrument(skip_all, name = "cohort-transfer")]
    pub async fn transfer(
        agent: Arc<AgentType>,
        database: Arc<Database>,
        from: &BankAccount,
        to: &BankAccount,
        amount: Decimal,
        cpt_snapshot: Snapshot,
    ) -> Result<BankResult, String> {
        let (shanpshot_version, read_vers) = Self::select_snapshot_and_readvers(cpt_snapshot.version, vec![from.version, to.version]);
        let xid = uuid::Uuid::new_v4().to_string();
        let statemap = vec![HashMap::from([(
            BusinessActionType::TRANSFER.to_string(),
            TransferRequest::new(from.number.clone(), to.number.clone(), amount).json(),
        )])];
        let cert_req = CertificationRequest {
            message_key: "cohort-sample".to_string(),
            candidate: CandidateData {
                xid: xid.clone(),
                readset: vec![from.number.to_string(), to.number.to_string()],
                readvers: read_vers,
                snapshot: shanpshot_version,
                writeset: vec![from.number.to_string(), to.number.to_string()],
                statemap: Some(statemap),
            },
            timeout: None,
        };

        // span 3.1 (agent.certify)
        let s1_cert_s = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let rslt_cert = agent.certify(cert_req).await;
        let s1_cert_f = OffsetDateTime::now_utc().unix_timestamp_nanos();

        if let Err(e) = rslt_cert {
            log::warn!(
                "Error communicating via agent: {}, xid: {}, operation: 'transfer' {} from {} to {}",
                e.reason,
                xid,
                amount,
                from,
                to,
            );
            return Err(format!("{:?}", e));
        }

        let resp = rslt_cert.unwrap();
        if Decision::Aborted == resp.decision {
            log::debug!("Aborted by talos: xid: {}, operation: 'transfer' {} from {} to {}", xid, amount, from, to);
            return Ok(BankResult {
                is_aborted: true,
                is_tx_isolation_error: false,
                metrics: TxExecSpans {
                    span1_get_accounts: Span::new(0, 0),
                    span2_get_snap_ver: Span::new(0, 0),
                    span3_certify: Span::new(s1_cert_s, s1_cert_f),
                    span4_wait_for_safepoint: Span::new(0, 0),
                    span5_install: Span::new(0, 0),
                },
            });
        }

        // log::info!("Running: 'T' {} {} {}", from.number.clone(), amount.clone(), to.number.clone());
        // Check safepoint condition before installing ...

        let safepoint = resp.safepoint.unwrap(); // this is safe for 'Committed'
        log::debug!(
            "... waiting for safepoint: {} on 'T' {} {} {}",
            safepoint,
            from.number.clone(),
            amount.clone(),
            to.number.clone()
        );

        // span 3.2 (SnapshotApi::await_until_safe)
        let s2_await_s = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let await_details = SnapshotApi::await_until_safe(Arc::clone(&database), safepoint).await?;
        let s2_await_f = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let elapsed = Duration::from_nanos((s2_await_f - s2_await_s) as u64);
        if elapsed.as_secs() >= 15 {
            let (db, sleeps, loops) = await_details;
            log::warn!(
                "long-safepoint-wait: {},{},{},{}",
                db.as_micros(),
                sleeps.as_micros(),
                loops,
                elapsed.as_micros()
            )
        }

        let mut res = Ok(BankResult {
            is_aborted: false,
            is_tx_isolation_error: false,
            metrics: TxExecSpans {
                span1_get_accounts: Span::new(0, 0),
                span2_get_snap_ver: Span::new(0, 0),
                span3_certify: Span::new(s1_cert_s, s1_cert_f),
                span4_wait_for_safepoint: Span::new(s2_await_s, s2_await_f),
                span5_install: Span::new(0, 0),
            },
        });

        for _ in 1..=2 {
            // span 3.3 (BankApi::transfer)
            let s3_transfer_s = OffsetDateTime::now_utc().unix_timestamp_nanos();
            let rslt = BankApi::transfer(
                Arc::clone(&database),
                TransferRequest::new(from.number.clone(), to.number.clone(), amount),
                resp.version,
            )
            .await;
            let s3_transfer_f = OffsetDateTime::now_utc().unix_timestamp_nanos();

            if rslt.is_err() {
                let e = rslt.unwrap_err();
                if e.contains("could not serialize access due to concurrent update")
                    || e.contains("could not serialize access due to read/write dependencies among transactions")
                {
                    log::debug!(
                        "tx conflict when running 'T' {} {} {}. Err: {}, Moving on...\n",
                        from.number.clone(),
                        amount.clone(),
                        to.number.clone(),
                        e
                    );

                    res = Ok(BankResult {
                        is_aborted: false,
                        is_tx_isolation_error: true,
                        metrics: TxExecSpans {
                            span1_get_accounts: Span::new(0, 0),
                            span2_get_snap_ver: Span::new(0, 0),
                            span3_certify: Span::new(s1_cert_s, s1_cert_f),
                            span4_wait_for_safepoint: Span::new(s2_await_s, s2_await_f),
                            span5_install: Span::new(s3_transfer_s, s3_transfer_f),
                        },
                    });
                } else {
                    res = Err(e);

                    break;
                }
            } else {
                log::debug!(
                    "updated {} rows when running 'T' {} {} {}\n",
                    rslt.unwrap(),
                    from.number.clone(),
                    amount.clone(),
                    to.number.clone()
                );

                res = Ok(BankResult {
                    is_aborted: false,
                    is_tx_isolation_error: false,
                    metrics: TxExecSpans {
                        span1_get_accounts: Span::new(0, 0),
                        span2_get_snap_ver: Span::new(0, 0),
                        span3_certify: Span::new(s1_cert_s, s1_cert_f),
                        span4_wait_for_safepoint: Span::new(s2_await_s, s2_await_f),
                        span5_install: Span::new(s3_transfer_s, s3_transfer_f),
                    },
                });

                break;
            }
        }
        // log::warn!("transfer() installed.");

        res
    }
    // $coverage:ignore-end

    fn select_snapshot_and_readvers(cpt_snapshot: u64, cpt_versions: Vec<u64>) -> (u64, Vec<u64>) {
        log::debug!("select_snapshot_and_readvers({}, {:?})", cpt_snapshot, cpt_versions);
        if cpt_versions.is_empty() {
            log::debug!(
                "select_snapshot_and_readvers({}, {:?}): {:?}",
                cpt_snapshot,
                cpt_versions,
                (cpt_snapshot, Vec::<u64>::new())
            );
            return (cpt_snapshot, vec![]);
        }

        let mut cpt_version_min: u64 = u64::MAX;
        for v in cpt_versions.iter() {
            if cpt_version_min > *v {
                cpt_version_min = *v;
            }
        }
        let snapshot_version = std::cmp::max(cpt_snapshot, cpt_version_min);
        let mut read_vers = Vec::<u64>::new();
        for v in cpt_versions.iter() {
            if snapshot_version < *v {
                read_vers.push(*v);
            }
        }

        log::debug!(
            "select_snapshot_and_readvers({}, {:?}): {:?}",
            cpt_snapshot,
            cpt_versions,
            (snapshot_version, read_vers.clone())
        );
        (snapshot_version, read_vers)
    }
}

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use crate::core::Cohort;

    #[test]
    fn select_snapshot_and_readvers() {
        let (s, rv) = Cohort::select_snapshot_and_readvers(1, vec![]);
        assert_eq!(s, 1);
        assert!(rv.is_empty());

        let (s, rv) = Cohort::select_snapshot_and_readvers(1, vec![1]);
        assert_eq!(s, 1);
        assert!(rv.is_empty());

        let (s, rv) = Cohort::select_snapshot_and_readvers(2, vec![1, 2, 2]);
        assert_eq!(s, 2);
        assert!(rv.is_empty());

        let (s, rv) = Cohort::select_snapshot_and_readvers(2, vec![1, 2, 3, 4]);
        assert_eq!(s, 2);
        assert_eq!(rv, vec![3, 4]);

        let (s, rv) = Cohort::select_snapshot_and_readvers(5, vec![1, 2, 3, 4]);
        assert_eq!(s, 5);
        assert!(rv.is_empty());
    }
}
// $coverage:ignore-end
