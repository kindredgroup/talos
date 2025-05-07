// $coverage:ignore-start

use std::time::Duration;

use opentelemetry::metrics::{Gauge, Histogram, Meter, UpDownCounter};
use talos_common_utils::sync::{try_send_with_retry, TrySendWithRetryConfig};
use time::OffsetDateTime;
use tokio::{sync::mpsc, time::Interval};
use tracing::{error, info};

use crate::{
    callbacks::ReplicatorSnapshotProvider,
    core::{ReplicatorChannel, StatemapInstallState, StatemapInstallationStatus, StatemapInstallerHashmap, StatemapItem, StatemapQueueChannelMessage},
    errors::{ReplicatorError, ReplicatorErrorKind},
    models::StatemapInstallerQueue,
};

#[derive(Debug)]
pub struct StatemapQueueServiceConfig {
    pub queue_cleanup_frequency_ms: u64,
    pub enable_stats: bool,
}

impl Default for StatemapQueueServiceConfig {
    fn default() -> Self {
        Self {
            queue_cleanup_frequency_ms: 10_000,
            enable_stats: false,
        }
    }
}

#[derive(Debug, Default)]
struct Metrics {
    enabled: bool,
    h_installation_latency: Option<Histogram<f64>>,
    g_installation_tx_channel_usage: Option<Gauge<u64>>,
    g_statemap_queue_length: Option<Gauge<u64>>,
    g_snapshot_version: Option<Gauge<u64>>,
    udc_items_in_flight: Option<UpDownCounter<i64>>,
    channel_size: u64,

    // Internal metrics for debugging
    // TODO: GK - remove at later time or export to telemetry the derived metrics.
    pub installation_success_count: u32,
    pub send_for_install_count: u32,
    pub first_install_start: i128,
    pub last_install_end: i128,
}

impl Metrics {
    pub fn new(meter: Option<Meter>, channel_size: u64) -> Self {
        let metrics_default = Self {
            channel_size,
            ..Self::default()
        };
        if let Some(meter) = meter {
            Self {
                enabled: true,
                h_installation_latency: Some(meter.f64_histogram("repl_statemap_queue_latency").build()),
                g_installation_tx_channel_usage: Some(meter.u64_gauge("repl_install_channel").with_unit("items").build()),
                g_statemap_queue_length: Some(meter.u64_gauge("repl_statemap_queue").with_unit("items").build()),
                g_snapshot_version: Some(meter.u64_gauge("repl_statemap_queue_snapshot").with_unit("items").build()),
                udc_items_in_flight: Some(meter.i64_up_down_counter("repl_items_in_flight").with_unit("items").build()),
                ..metrics_default
            }
        } else {
            metrics_default
        }
    }

    pub fn inflight_inc(&self) {
        let _ = self.udc_items_in_flight.as_ref().map(|m| m.add(1, &[]));
    }
    pub fn inflight_dec(&self) {
        let _ = self.udc_items_in_flight.as_ref().map(|m| m.add(-1, &[]));
    }
    pub fn record_snapshot(&self, snapshot: u64) {
        if self.enabled {
            let _ = self.g_snapshot_version.as_ref().map(|m| m.record(snapshot, &[]));
        }
    }
    pub fn record_sizes(&self, installation_tx_capacity: usize, queue_len: usize) {
        if self.enabled {
            let _ = self
                .g_installation_tx_channel_usage
                .as_ref()
                .map(|m| m.record(self.channel_size - installation_tx_capacity as u64, &[]));
            let _ = self.g_statemap_queue_length.as_ref().map(|m| m.record(queue_len as u64, &[]));
        }
    }
    pub fn record_latency(&self, latency: Option<Duration>) {
        if let Some(latency) = latency {
            let latency_ms = latency.as_nanos() as f64 / 1_000_000_f64;
            let _ = self.h_installation_latency.as_ref().map(|metric| metric.record(latency_ms, &[]));
        }
    }
}

pub struct StatemapQueueService<S>
where
    S: ReplicatorSnapshotProvider + Send + Sync,
{
    statemaps_rx: mpsc::Receiver<StatemapQueueChannelMessage>,
    installation_tx: mpsc::Sender<(u64, Vec<StatemapItem>)>,
    installation_feedback_rx: mpsc::Receiver<StatemapInstallationStatus>,
    replicator_feedback: mpsc::Sender<ReplicatorChannel>,
    pub statemap_queue: StatemapInstallerQueue,
    snapshot_api: S,
    cleanup_interval: Interval,
    config: StatemapQueueServiceConfig,
    metrics: Metrics,
}

impl<S> StatemapQueueService<S>
where
    S: ReplicatorSnapshotProvider + Send + Sync,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        statemaps_rx: mpsc::Receiver<StatemapQueueChannelMessage>,
        installation_tx: mpsc::Sender<(u64, Vec<StatemapItem>)>,
        installation_feedback_rx: mpsc::Receiver<StatemapInstallationStatus>,
        replicator_feedback_tx: mpsc::Sender<ReplicatorChannel>,
        snapshot_api: S,
        config: StatemapQueueServiceConfig,
        channel_size: usize,
        otel_meter: Option<Meter>,
    ) -> Self {
        let metrics = Metrics::new(otel_meter, channel_size as u64);
        let interval_ms = config.queue_cleanup_frequency_ms;
        Self {
            statemaps_rx,
            installation_tx,
            installation_feedback_rx,
            replicator_feedback: replicator_feedback_tx,
            snapshot_api,
            config,
            metrics,
            statemap_queue: StatemapInstallerQueue::default(),
            cleanup_interval: tokio::time::interval(Duration::from_millis(interval_ms)),
        }
    }

    async fn get_latest_snapshot_from_callback(&mut self) -> Result<u64, ReplicatorError> {
        self.snapshot_api.get_snapshot().await.map_err(|e| ReplicatorError {
            kind: ReplicatorErrorKind::Persistence,
            reason: "Failed to get snapshot version from db".into(),
            cause: Some(e.to_string()),
        })
    }

    fn update_snapshot(&mut self, snapshot_version: u64) -> Option<u64> {
        if snapshot_version > self.statemap_queue.snapshot_version {
            info!("Updating snapshot version from {} to {snapshot_version}", self.statemap_queue.snapshot_version);
            self.statemap_queue.update_snapshot(snapshot_version);

            self.metrics.record_snapshot(snapshot_version);
            Some(snapshot_version)
        } else {
            None
        }
    }

    /// If a version is not on the queue, get the nearest version below the provided version from queue.
    fn get_nearest_valid_version(&self, version: u64) -> Option<u64> {
        if self.statemap_queue.queue.get_index_of(&version).is_some() {
            return Some(version);
        }

        if let Some((last_below_version, _)) = self.statemap_queue.queue.iter().take_while(|(v, _)| **v < version).last() {
            if self.statemap_queue.queue.get_index_of(last_below_version).is_some() {
                return Some(*last_below_version);
            }
        }

        None
    }

    /// Gets the statemaps eligible to be installed and sends it over the channel to the **_installer service_**.
    async fn send_statemaps_for_install(&mut self) {
        // Gets the statemaps to send for installation.
        let items_to_install: Vec<u64> = self.statemap_queue.get_versions_to_install();

        // Sends for installation.
        for key in items_to_install {
            // Send for installation
            if self.metrics.send_for_install_count == 0 {
                self.metrics.first_install_start = OffsetDateTime::now_utc().unix_timestamp_nanos();
            }
            self.metrics.send_for_install_count += 1;
            if let Some(item) = self.statemap_queue.queue.get(&key) {
                match self.installation_tx.send((key, item.statemaps.clone())).await {
                    Ok(_) => {
                        self.metrics.inflight_inc();
                        self.statemap_queue.update_queue_item_state(&key, StatemapInstallState::Inflight);
                    }
                    Err(err) => {
                        error!("Failed to send statemaps of version {key}. Error {err:?}");
                        // If there is error, stop sending further, and they will be picked again to be send.
                        break;
                    }
                }
            }
            // Update the status flag
        }
    }

    pub async fn run_once(&mut self) -> Result<(), ReplicatorError> {
        tokio::select! {
            statemap_channel_message = self.statemaps_rx.recv() => {
                // Insert messages into the internal queue and set the status to `StatemapInstallState::Awaiting`
                // Pick statemaps eligible to be installed and send to `statemap_installer_service`, and set their state to `StatemapInstallState::Inflight`.
                match statemap_channel_message {
                    Some(StatemapQueueChannelMessage::Message((version, statemaps))) => {

                        // Inserts the statemaps to the map

                        // Get the safepoint.
                        let safepoint = if let Some(first_statemap) = statemaps.first() {
                            first_statemap.safepoint
                        } else {
                            None
                        };

                        // If version is below the snapshot_version, it is already installed, hence insert and mark it as installed.
                        let state = if version < self.statemap_queue.snapshot_version {
                            StatemapInstallState::Installed
                        } else {
                            StatemapInstallState::Awaiting
                        };

                        self.statemap_queue.insert_queue_item(&version, StatemapInstallerHashmap {
                            timestamp: OffsetDateTime::now_utc().unix_timestamp_nanos(),
                            statemaps,
                            version,
                            safepoint,
                            state
                        });


                    },
                    // Update the snapshot value.
                    Some(StatemapQueueChannelMessage::UpdateSnapshot) => {
                        info!("Fetching latest snapshot version from callback");
                        let snapshot_version_from_callback = self.get_latest_snapshot_from_callback().await?;

                        // Update the snapshot with the latest from callback only if it is greater than our internal snapshot tracker.
                        if self.update_snapshot(snapshot_version_from_callback).is_some() {
                            // The snapshot version we updated may not be present in the queue, so we get the nearest one below this snapshot version and prune till that version
                            // If there are no versions below this one, then there is no need to prune.
                            if let Some(version) = self.get_nearest_valid_version(snapshot_version_from_callback) {
                                // prune items till the specified version.
                                self.statemap_queue.prune_till_version(version);
                                // Inform replicator service to remove all versions below this index.
                                if let Err(err) = try_send_with_retry(&self.replicator_feedback, ReplicatorChannel::LastInstalledVersion(version), TrySendWithRetryConfig::default()).await {
                                    error!("Failed to send LastInstalledVersion {version} with error {err:?}");
                                }

                            };

                        }

                    },

                    None => {},
                }

                // Get statemap items from queue and send it for installation.
                self.send_statemaps_for_install().await;

                self.metrics.record_sizes(self.installation_tx.capacity(), self.statemap_queue.queue.len());

            }
            Some(install_result) = self.installation_feedback_rx.recv() => {
                match install_result {
                    StatemapInstallationStatus::Success(version) => {
                        self.metrics.inflight_dec();
                        // installed successfully and will remove the item
                        let enc_time = self.statemap_queue.update_queue_item_state(&version, StatemapInstallState::Installed);
                        self.metrics.record_latency(enc_time.map(|enqueue_time_nanos| Duration::from_nanos((OffsetDateTime::now_utc().unix_timestamp_nanos() - enqueue_time_nanos) as u64)));

                        if let Some(version) = self.statemap_queue.get_last_contiguous_installed_version(){
                            if self.update_snapshot(version).is_some() {
                                // Inform replicator service to remove all versions below this.
                                if let Err(err) = try_send_with_retry(&self.replicator_feedback, ReplicatorChannel::LastInstalledVersion(self.statemap_queue.snapshot_version), TrySendWithRetryConfig::default()).await {
                                    error!("Failed to send LastInstalledVersion {} with error {err:?}", self.statemap_queue.snapshot_version);
                                }
                            }
                        };

                        self.metrics.installation_success_count += 1;
                        self.metrics.last_install_end = OffsetDateTime::now_utc().unix_timestamp_nanos();

                    },
                    StatemapInstallationStatus::Error(ver, error) => {
                        self.metrics.inflight_dec();
                        error!("Failed to install version={ver} due to error={error:?}");
                        // set the item back to awaiting so that it will be picked again for installation.
                        let enc_time = self.statemap_queue.update_queue_item_state(&ver, StatemapInstallState::Awaiting);
                        self.metrics.record_latency(enc_time.map(|enqueue_time_nanos| Duration::from_nanos((OffsetDateTime::now_utc().unix_timestamp_nanos() - enqueue_time_nanos) as u64)));
                    },
                }

                // Get statemap items from queue and send it for installation.
                self.send_statemaps_for_install().await;

                self.metrics.record_sizes(self.installation_tx.capacity(), self.statemap_queue.queue.len());
            }
            _ = self.cleanup_interval.tick() => {

                let mut last_version = None;
                if let Some(version) = self.statemap_queue.get_last_contiguous_installed_version(){
                    last_version = Some(version);
                    self.update_snapshot(version);

                };
                let result = self.statemap_queue.prune_till_version(self.statemap_queue.snapshot_version);
                self.metrics.record_sizes(self.installation_tx.capacity(), self.statemap_queue.queue.len());
                // Inform replicator service to remove all versions below this.
                if let Err(err) = try_send_with_retry(&self.replicator_feedback, ReplicatorChannel::LastInstalledVersion(self.statemap_queue.snapshot_version), TrySendWithRetryConfig::default()).await {
                    error!("Failed to send LastInstalledVersion {} with error {err:?}", self.statemap_queue.snapshot_version);
                }

                if result.is_some() {
                    info!("Pruned {:?} items from queue | snapshot_version = {} ", result, self.statemap_queue.snapshot_version);
                }


                if self.config.enable_stats {
                    let duration_sec = Duration::from_nanos((self.metrics.last_install_end - self.metrics.first_install_start) as u64).as_secs_f32();
                    let tps = self.metrics.installation_success_count as f32 / duration_sec;

                    let awaiting_count = self.statemap_queue.filter_items_by_state(StatemapInstallState::Awaiting).count();
                    let inflight_count = self.statemap_queue.filter_items_by_state(StatemapInstallState::Inflight).count();

                    info!("Statemap Queue Service stats:- tps = {tps:.3} | Count of items in AWAITING state = {awaiting_count} |  Count of items in INFLIGHT state = {inflight_count} | Count of items in INSTALLED state = {} | statemap queue length = {}",self.metrics.installation_success_count, self.statemap_queue.queue.len());
                    if self.statemap_queue.queue.is_empty() {
                        let first_item = self.statemap_queue.queue.first();
                        let last_item = self.statemap_queue.queue.last();

                        info!(
                            "First item in queue = {} with state = {:?} | Last item in queue = {} with state = {:?} | snapshot_version = {} | last_contiguous_install_version in this interval tick = {last_version:?}",
                            first_item.unwrap().0,
                            first_item.unwrap().1.state,
                            last_item.unwrap().0,
                            last_item.unwrap().1.state,
                            self.statemap_queue.snapshot_version

                        );
                    }

                    if self.config.enable_stats && awaiting_count > 0 && inflight_count == 0
                    {
                        let result = self.statemap_queue.dbg_get_versions_to_install();
                        let final_items = result.installable_items;
                        let criteria_awaiting_state = &result.filter_steps_insights[0];
                        let criteria_snapshot_check = &result.filter_steps_insights[1];
                        let criteria_seriazable_check = &result.filter_steps_insights[2];

                        if let Some(first_item_to_fail_safepoint_check) = criteria_snapshot_check.filter_reject_items.first() {
                            info!("\n\n
                +----------+-----------------------------------+----------------------------+
                | Total    | Items in queue                    | {}
                +----------+-----------------------------------+----------------------------+
                | Filter 1 | Items in AWAITING state           | {}
                | Filter 2 | Items whose safepoint <= snapshot | {}
                |          |                                   | Current snapshot={}
                |          |-----------------------------------+----------------------------+
                |          |  > First item to fail this check  | Version={}
                |          |                                   | Safepoint={:?}
                |          |-----------------------------------+----------------------------+
                | Filter 3 | Items serializable in the batch   | {}
                |          |  > Non serializable items count   | {}
                +----------+-----------------------------------+------+---------------------+
                | Total    | Items ready to install            | {}
                +----------+-----------------------------------+----------------------------+
                            \n\n",
                            criteria_awaiting_state.filter_enter_count,
                            criteria_awaiting_state.filter_exit_count,
                            criteria_snapshot_check.filter_exit_count,
                            self.statemap_queue.snapshot_version,
                            first_item_to_fail_safepoint_check.version,
                            first_item_to_fail_safepoint_check.safepoint,
                            criteria_seriazable_check.filter_exit_count,
                            criteria_seriazable_check.filter_reject_items.len(),
                            final_items.len(),
                        );
                        }
                    }
                }



                self.metrics.record_sizes(self.installation_tx.capacity(), self.statemap_queue.queue.len());
            }

        }
        Ok(())
    }

    pub async fn run(&mut self) -> Result<(), ReplicatorError> {
        info!("Starting Statemap Queue Service.... ");

        //Gets snapshot initial version from db.
        let snapshot_version_from_db = self.get_latest_snapshot_from_callback().await?;
        self.update_snapshot(snapshot_version_from_db);

        loop {
            self.run_once().await?
        }
    }
}
// $coverage:ignore-end
