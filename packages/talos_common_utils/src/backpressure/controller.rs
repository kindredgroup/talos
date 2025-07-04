use std::ops::Sub;

use time::OffsetDateTime;
use tracing::debug;

use super::config::BackPressureConfig;

#[derive(Debug, Clone, PartialEq)]
pub enum BackPressureTimeout {
    /// No timeout to be applied.
    NoTimeout,
    /// General timeout. With timeout in milliseconds
    Timeout(u64),
    /// Critical scenario to completely stop incoming messages. Along with max_timeout.
    CriticalStop(u64),
}

#[derive(Debug, Default, Clone)]
pub struct BackPressureVersionTracker {
    pub version: u64,
    pub time_ns: i128,
}

impl BackPressureVersionTracker {
    pub fn new(version: u64, time_ns: i128) -> Self {
        Self { version, time_ns }
    }

    pub fn update(&mut self, version: u64, time_ns: i128) {
        self.version = version;
        self.time_ns = time_ns;
    }

    pub fn reset(&mut self) {
        self.update(0, 0);
    }
}

/// Back pressure logic for Talos. Since all core talos services like certifier, messenger or replicator uses an abcast to receive the certification message,
/// which in turn hydrates the suffix, there is always a risk of suffix growingly to quick, increasing the chance of OOM, if they are not pruned quickly.
/// To control this, there are two strategies baked into this the TalosBackPressure logic.
/// - Reactive - When the suffix grows beyond a threshold, compute the time to wait before consuming new message and hydrating the suffix.
/// - Proactive - When the output rate is falling below a threshold with respect to the input rate, proactively wait on consuming new messages, thereby maintaining an optimal balance between the input and output.
///
/// **NOTE 1:-** Configuring the back pressure to kick in often or for false positives, can impact the latency and throughput.
/// Therefore tweak the configs to best suit different use case.
///
/// **NOTE 2:-** The reason why a sliding window based approach was not used for the backpressure strategy is because, the computation and storing of historical input and output has overhead on CPU and memory.
/// For a high through low latency system, we want to avoid anything that can impact the performance as much as possible. Therefore this may be implemented at a later stage if the current logic has gaps.
/// The accuracy using sliding window is more, but with added complexity and potential impact on performance. The current logic is designed in a way to act when certain thresholds have crossed in the suffix,
/// the assumptions is this would be good enough for most scenarios.
///

#[derive(Debug, Default)]
pub struct BackPressureController {
    /// Config used to control the backpressure
    pub config: BackPressureConfig,
    /// first suffix head during the check window.
    pub first_suffix_head: BackPressureVersionTracker,
    /// last suffix head during the check window.
    pub last_suffix_head: BackPressureVersionTracker,
    /// first candidate received during the check window.
    pub first_candidate_received: BackPressureVersionTracker,
    /// last candidate received during the check window.
    pub last_candidate_received: BackPressureVersionTracker,
    /// An internal tracker which is updated any time the head tracker is updated.
    ///
    /// **NOTE** - This should not be reset.
    last_head_updated_ns: i128,
}

impl BackPressureController {
    pub fn with_config(config: BackPressureConfig) -> Self {
        Self { config, ..Default::default() }
    }

    /// Reset the version trackers used to calculate the input and output rate.
    pub fn reset_version_trackers(&mut self) {
        self.first_suffix_head.reset();
        self.last_suffix_head.reset();
        self.first_candidate_received.reset();
        self.last_candidate_received.reset();
    }

    /// Calculate the suffix fill ratio using the current suffix size against `config.suffix_max_size`
    fn get_suffix_fill_ratio(&self, current_suffix_size: u64) -> f64 {
        // Determine the ratio of how much of suffix is filled by checking the current size againt the `TalosBackPressureConfig.suffix_max_length`
        let suffix_fill_ratio = current_suffix_size as f64 / self.config.suffix_max_size as f64;
        // Clamp it between 0.0 and 1.0. i.e Empty to full.
        suffix_fill_ratio.clamp(0.0, 1.0)
    }

    pub fn update_suffix_head_trackers(&mut self, tracker: BackPressureVersionTracker) {
        if self.first_suffix_head.version == 0 {
            self.first_suffix_head = tracker.clone();
        }
        self.last_head_updated_ns = tracker.time_ns;
        self.last_suffix_head = tracker;
    }

    pub fn update_candidate_received_tracker(&mut self, tracker: BackPressureVersionTracker) {
        if self.first_candidate_received.version == 0 {
            self.first_candidate_received = tracker.clone();
        }
        self.last_candidate_received = tracker;
    }

    /// Computation for reactively applying backpressure based on rate gap between input and output rate.
    pub(crate) fn compute_suffix_fill_score(&self, suffix_fill_ratio: f64) -> f64 {
        // The threshold beyond which the backpressure based on suffix filling should be applied.
        let suffix_fill_weight_threshold = self.config.suffix_fill_threshold;

        // Map how much is filled over the threshold on a scale of 0-1.
        let score_to_threshold = suffix_fill_ratio.sub(suffix_fill_weight_threshold).max(0.0);
        let suffix_fill_score = score_to_threshold / (1.0 - suffix_fill_weight_threshold);
        debug!("Backpressure fill score compute - suffix_fill_ratio = {suffix_fill_ratio} | suffix_fill_weight_threshold = {suffix_fill_weight_threshold} | suffix_fill_score = {suffix_fill_score}");

        suffix_fill_score
    }

    /// Computation for proactively applying backpressure based on rate gap between input and output rate.
    pub(crate) fn compute_rate_score(&self) -> f64 {
        // The delta threshold between the input rate and output rate. If the rate goes beyond this threshold, we need to apply backpressure based on rate (proactive)
        let tps_threshold = self.config.rate_delta_threshold;

        // Compute the input rate based on the first and last candidate during a window
        let input_time_sec = (self.last_candidate_received.time_ns - self.first_candidate_received.time_ns) as f64 / 1_000_000_000_f64;
        let input_rate = (self.last_candidate_received.version - self.first_candidate_received.version) as f64 / input_time_sec;

        // Compute the output rate. The rate at which the head is moving helps to determine the output rate.
        let output_time_sec = (self.last_suffix_head.time_ns - self.first_suffix_head.time_ns) as f64 / 1_000_000_000_f64;
        let output_rate = (self.last_suffix_head.version - self.first_suffix_head.version) as f64 / output_time_sec;

        let delta_rate = input_rate.sub(output_rate);

        let rate_score = match tps_threshold {
            Some(threshold) if delta_rate > threshold => {
                let log_base = threshold.log10();
                let scaled = delta_rate.log10() - log_base;

                scaled.clamp(0.0, 1.0)
            }
            _ => 0.0,
        };
        debug!(
            "Backpressure rate score compute - Input rate = {input_rate} | Output rate = {output_rate} | delta_rate = {delta_rate} | rate_score = {rate_score}"
        );

        rate_score
    }

    /// Calculates the timeout in milliseconds.
    ///
    /// - Uses a `fill_score` to determine if the suffix size should be considered for the timeout calculation.
    /// - Uses a `rate_score` to determine if the output rate is not keeping up with the input rate.
    /// - Both the scores are used to determine the timeout to be applied, which will be between
    /// [`BackPressureConfig::min_timeout_ms`] and [`BackPressureConfig::max_timeout_ms`]
    pub fn calculate_timeout(&self, current_suffix_size: u64) -> u64 {
        let suffix_fill_ratio = self.get_suffix_fill_ratio(current_suffix_size);

        let suffix_fill_score = self.compute_suffix_fill_score(suffix_fill_ratio);
        let rate_score = if suffix_fill_ratio >= self.config.suffix_rate_threshold {
            self.compute_rate_score()
        } else {
            0.0
        };

        // Check if we're below both thresholds (no backpressure needed)
        if suffix_fill_score <= 0.0 && rate_score <= 0.0 {
            return 0;
        }

        // Calculate minimum and maximum timeouts
        let min_timeout_ms = self.config.min_timeout_ms;
        let max_timeout_ms = self.config.max_timeout_ms;

        // Both suffix fill and rate thresholds exceeded - combine both scores
        let suffix_fill_weighted = suffix_fill_score.powf(1.5);
        let rate_weighted = 1.0 - suffix_fill_weighted;
        let combined_score = suffix_fill_weighted + (rate_weighted * rate_score);

        let timeout_ms = min_timeout_ms as f64 + ((max_timeout_ms - min_timeout_ms) as f64 * combined_score);
        let timeout_ms = (timeout_ms.round() as u64).clamp(min_timeout_ms, max_timeout_ms);

        if timeout_ms > 0 {
            debug!("Backpressure timeout calculation -  suffix_fill_weighted = {suffix_fill_weighted} | rate_weighted = {rate_weighted} | rate_score = {rate_score} | timeout_ms = {timeout_ms}");
        }

        timeout_ms
    }

    /// Using the provided timeout, compute a new timeout value which will be stepped down using [`BackPressureConfig::timeout_stepdown_rate`]
    /// and ensures the value is not below the [`BackPressureConfig::min_timeout_ms`]
    pub fn compute_stepdown_timeout(&self, timeout_ms: u64) -> u64 {
        ((timeout_ms as f64 * self.config.timeout_stepdown_rate).round() as u64).max(self.config.min_timeout_ms)
    }

    /// Compute the backpressure
    ///
    /// - Calculate the timeout in milliseconds using [`Self::calculate_timeout`].
    /// - Reset the trackers. As the rates are calculated for a particular time window.
    /// - Return [`BackPressureTimeout`], based on the whether timeout needs to be applied, and/or if the head is stale for long,
    /// return [`BackPressureTimeout::CompleteStop`]
    pub fn compute_backpressure(&mut self, current_suffix_len: u64) -> BackPressureTimeout {
        // calculate the timeout to apply for backpressure.
        let timeout_ms = self.calculate_timeout(current_suffix_len);

        // reset the version trackers
        self.reset_version_trackers();

        // No timeout
        if timeout_ms == 0 {
            BackPressureTimeout::NoTimeout
        } else {
            // head is not moving for a long time, then we stop proceeding
            if timeout_ms == self.config.max_timeout_ms {
                let stale_head_time_ms = (OffsetDateTime::now_utc().unix_timestamp_nanos() - self.last_head_updated_ns) / 1_000_000;
                if (stale_head_time_ms as u64) > self.config.max_head_stale_timeout_ms {
                    return BackPressureTimeout::CriticalStop(self.config.max_timeout_ms);
                }
            }
            // Timeout between min and max
            BackPressureTimeout::Timeout(timeout_ms)
        }
    }
}
