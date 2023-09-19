mod callback_impl;

use std::sync::Arc;

use napi::threadsafe_function::ThreadsafeFunction;
use napi_derive::napi;

use serde_json::Value;
use talos_certifier::ports::MessageReciever;
use talos_certifier_adapters::KafkaConsumer;
use talos_cohort_replicator::{talos_cohort_replicator, CohortReplicatorConfig, StatemapItem};

use crate::{map_error_to_napi_error, models::JsKafkaConfig};

use self::callback_impl::{SnapshotProviderDelegate, StatemapInstallerDelegate};

#[derive(Clone)]
#[napi(object)]
pub struct JsReplicatorConfig {
    pub enable_stats: bool,
    pub channel_size: i64,
    pub suffix_capacity: i64,
    pub suffix_prune_threshold: Option<i64>,
    pub suffix_minimum_size_on_prune: Option<i64>,
    pub certifier_message_receiver_commit_freq_ms: i64,
    pub statemap_queue_cleanup_freq_ms: i64,
    pub statemap_installer_threadpool: i64,
}

impl From<JsReplicatorConfig> for CohortReplicatorConfig {
    fn from(val: JsReplicatorConfig) -> Self {
        Self {
            enable_stats: val.enable_stats,
            channel_size: val.channel_size as usize,
            suffix_capacity: val.suffix_capacity as usize,
            suffix_prune_threshold: val.suffix_prune_threshold.map(|v| v as usize),
            suffix_minimum_size_on_prune: val.suffix_minimum_size_on_prune.map(|v| v as usize),
            certifier_message_receiver_commit_freq_ms: val.certifier_message_receiver_commit_freq_ms as u64,
            statemap_queue_cleanup_freq_ms: val.statemap_queue_cleanup_freq_ms as u64,
            statemap_installer_threadpool: val.statemap_installer_threadpool as u64,
        }
    }
}

#[napi(object)]
pub struct JsStatemapItem {
    pub action: String,
    pub version: i64,
    pub safepoint: Option<i64>,
    pub payload: Value,
}

impl From<StatemapItem> for JsStatemapItem {
    fn from(val: StatemapItem) -> Self {
        Self {
            action: val.action,
            version: val.version as i64,
            payload: val.payload,
            safepoint: val.safepoint.map(|v| v as i64),
        }
    }
}

#[napi(object)]
pub struct JsStatemapAndSnapshot {
    pub statemap: Vec<JsStatemapItem>,
    pub version: i64,
}

#[napi]
pub struct InternalReplicator {
    kafka_config: JsKafkaConfig,
    config: JsReplicatorConfig,
}

#[napi]
impl InternalReplicator {
    #[napi]
    pub async fn init(kafka_config: JsKafkaConfig, config: JsReplicatorConfig) -> napi::Result<InternalReplicator> {
        env_logger::builder().format_timestamp_millis().init();
        Ok(InternalReplicator { kafka_config, config })
    }

    #[napi]
    pub async fn run(
        &self,
        #[napi(ts_arg_type = "() => Promise<number>")] snapshot_provider_callback: ThreadsafeFunction<()>,
        statemap_installer_callback: ThreadsafeFunction<JsStatemapAndSnapshot>,
    ) -> napi::Result<()> {
        let kafka_consumer = KafkaConsumer::new(&self.kafka_config.clone().into());
        kafka_consumer.subscribe().await.map_err(map_error_to_napi_error)?;

        let config: CohortReplicatorConfig = self.config.clone().into();
        let _service_handle = tokio::spawn(async move {
            let _result = talos_cohort_replicator(
                kafka_consumer,
                Arc::new(StatemapInstallerDelegate {
                    callback: statemap_installer_callback,
                }),
                SnapshotProviderDelegate {
                    callback: snapshot_provider_callback,
                },
                config,
            )
            .await;
        });

        Ok(())
    }
}
