use std::ops::Sub;

use time::OffsetDateTime;

#[derive(Debug)]
pub struct TalosBackPressureConfig {
    /// Upper limit for timeout while calculating the back pressure in milliseconds.
    /// - **Defaults to 50ms.**
    max_timeout_ms: u64,
    /// Max length of suffix which is considered safe to avoid memory related issues.
    /// - **Defaults to 10_000.**
    suffix_max_length: u64,
    /// Suffix fill threshold is used enable back pressure logic based on the current length of suffix against `suffix_max_length`.
    /// The reactive back pressure strategy will not trigger till this threshold is crossed.
    /// - threshold should be between 0.0 and 1.0.
    /// - Values beyond this range will be clamped to lower or upper bound.
    /// - **Defaults to 0.7.** i.e when current length has reached `70%` of `suffix_max_length`.
    suffix_fill_threshold: f64,
    /// Suffix rate threshold is used enable back pressure logic based on the difference between input vs output rate, when the suffix has crossed this threshold.
    /// The proactive back pressure strategy will not trigger till this threshold is crossed.
    /// - threshold should be between 0.0 and `suffix_fill_threshold`.
    /// - Values beyond this range will be clamped to lower or upper bound.
    /// - **Defaults to 0.5.** i.e when current length has reached `50%` of `suffix_max_length`.
    suffix_rate_threshold: f64,
    /// Suffix rate threshold is used enable back pressure logic based on the difference between input vs output rate.
    /// The proactive back pressure strategy will not trigger till `suffix_rate_threshold` + this rate threshold is crossed.
    /// - If Some valid value is passed., the rate based proactive back pressure logic is applied only when this threshold is crossed.
    ///         - e.g. If `rate_delta_threshold = 100`, then proactive back pressure logic kicks in when `input_tps - output_tps > 100`.
    /// - **Defaults to `None`.** i.e rate based proactive back pressure will not run
    rate_delta_threshold: Option<f64>,
}

impl Default for TalosBackPressureConfig {
    fn default() -> Self {
        Self {
            max_timeout_ms: 50, //50ms
            suffix_max_length: 10_000,
            suffix_fill_threshold: 0.7,
            suffix_rate_threshold: 0.5,
            rate_delta_threshold: None,
        }
    }
}

impl TalosBackPressureConfig {
    pub fn new(
        max_timeout_ms: Option<u64>,
        suffix_max_length: Option<u64>,
        suffix_fill_threshold: Option<f64>,
        suffix_rate_threshold: Option<f64>,
        rate_delta_threshold: Option<f64>,
    ) -> Self {
        let suffix_fill_threshold = suffix_fill_threshold.unwrap_or_default().clamp(0.0, 1.0);
        let suffix_rate_threshold = suffix_rate_threshold.unwrap_or_default().clamp(0.0, suffix_fill_threshold);
        Self {
            max_timeout_ms: max_timeout_ms.unwrap_or_default(),
            suffix_max_length: suffix_max_length.unwrap_or_default(),
            suffix_fill_threshold,
            suffix_rate_threshold,
            rate_delta_threshold,
        }
    }

    pub fn get(&self) -> &Self {
        self
    }
}

#[derive(Debug, Default)]
pub struct TalosBackPressureVersionTracker {
    pub version: u64,
    pub time_ns: i128,
}

impl TalosBackPressureVersionTracker {
    pub fn update(&mut self, version: u64, time_ns: i128) {
        self.version = version;
        self.time_ns = time_ns;
    }
}

/// Back pressure logic for Talos. Since all core talos services like certifier, messenger or replicator uses an abcast to receive the certification message,
/// which in turn hydrates the suffix, there is always a risk of suffix growingly to quick, increasing the chance of OOM, if they are not pruned quickly.
/// To control this, there are two strategies baked into this the TalosBackPressure logic.
/// - Reactive - When the suffix grows beyond a threshold, compute the time to wait before consuming new message and hydrating the suffix.
/// - Proactive - When the output rate is falling below a threshold with respect to the input rate, proactively wait on consuming new messages, thereby maintaining an optimal balance between the input and output.
///
/// **NOTE:-** Configuring the back pressure to kick in often or for false positives, can impact the latency and throughput.
/// Therefore tweak the configs to best suit different use case.
#[derive(Debug, Default)]
pub struct TalosBackPressureController {
    pub config: TalosBackPressureConfig,
    pub last_suffix_head: TalosBackPressureVersionTracker,
    pub first_candidate_version: TalosBackPressureVersionTracker,
    pub last_candidate_version: TalosBackPressureVersionTracker,
}

impl TalosBackPressureController {
    pub fn with_config(config: TalosBackPressureConfig) -> Self {
        Self { config, ..Default::default() }
    }

    // pub fn update_last_head(&mut self, new_head: BackPressureVersionTracker) {
    //     self.last_suffix_head = new_head;
    // }

    // /// First check to see if further checks should be done
    // /// If true, that means it is safe and ignore rest of the checks
    // /// If false, then proceed to next checks
    // pub fn is_prune_happening(&mut self, current_head: u64) -> bool {
    //     if current_head != self.last_suffix_head.version {
    //         self.update_last_head(BackPressureVersionTracker {
    //             version: current_head,
    //             time_ns: OffsetDateTime::now_utc().unix_timestamp_nanos(),
    //         });
    //         true
    //     } else {
    //         false
    //     }
    // }

    // /// Check to see if size is above threshold
    // pub fn is_below_safe_threshold(&self, current_head: u64) -> bool {
    //     current_head <= self.config.size_threshold
    // }
    // /// Check to see if size is above threshold using a callback
    // pub fn is_below_safe_threshold_using_callback<T: Fn() -> u64>(&self, current_count_fn: T) -> bool {
    //     current_count_fn() <= self.config.size_threshold
    // }

    /// Computation for reactively applying backpressure based on rate gap between input and output rate.
    pub(crate) fn compute_suffix_fill_score(&self, current_suffix_len: u64) -> f64 {
        // Computation for reactive backpressure based on how full the suffix is.
        let suffix_fill_ratio = current_suffix_len as f64 / self.config.suffix_max_length as f64;
        let suffix_fill_ratio_clamped = suffix_fill_ratio.clamp(0.0, 1.0);

        let suffix_fill_weight_threshold = self.config.suffix_fill_threshold;

        let suffix_fill_score = if suffix_fill_ratio_clamped > suffix_fill_weight_threshold {
            (suffix_fill_ratio_clamped - suffix_fill_weight_threshold) / (1.0 - suffix_fill_weight_threshold)
            // fill_weight_threshold
        } else {
            0.0
        };
        println!(
            "
            | current_suffix_len = {current_suffix_len} | max_suffix_length= {}
            | fill_ratio ={suffix_fill_ratio} | fill_ratio_clamped = {suffix_fill_ratio_clamped}
            | suffix_fill_weight_threshold ={suffix_fill_weight_threshold} | suffix_fill_score = {suffix_fill_score}
            ",
            self.config.suffix_max_length
        );
        suffix_fill_score
    }

    /// Computation for proactively applying backpressure based on rate gap between input and output rate.
    pub(crate) fn compute_rate_score(&self, current_head: u64, current_time_ns: i128) -> f64 {
        let tps_threshold = self.config.rate_delta_threshold;

        // Get the input rate
        let input_time_sec = (self.last_candidate_version.time_ns - self.first_candidate_version.time_ns) as f64 / 1_000_000_000_f64;
        let input_rate = (self.last_candidate_version.version - self.first_candidate_version.version) as f64 / input_time_sec;

        // Get the rate at which the head is moving (approximates to output rate)
        let output_time_sec = (current_time_ns - self.last_suffix_head.time_ns) as f64 / 1_000_000_000_f64;
        let output_rate = (current_head - self.last_suffix_head.version) as f64 / output_time_sec;

        let rate_diff = input_rate.sub(output_rate);

        let rate_score = match tps_threshold {
            Some(threshold) if rate_diff >= threshold => {
                let log_base = threshold.log10();
                let scaled = rate_diff.log10() - log_base;
                scaled.clamp(0.0, 1.0)
            }
            _ => 0.0,
        };
        println!("Input rate = {input_rate} | Output rate = {output_rate} | rate_diff = {rate_diff} | rate_score = {rate_score}");

        rate_score
    }

    /// Compute the backpressure time in ms
    pub fn compute_backpressure_timeout(&mut self, current_head: u64, current_suffix_len: u64) -> u64 {
        let current_time_ns = OffsetDateTime::now_utc().unix_timestamp_nanos();

        let suffix_fill_score = self.compute_suffix_fill_score(current_suffix_len);
        let suffix_fill_weighted = suffix_fill_score.powf(2.0);

        let rate_score = self.compute_rate_score(current_head, current_time_ns);
        let rate_weighted = 1.0 - suffix_fill_weighted;

        // Final computation
        let backoff_score = (suffix_fill_weighted * suffix_fill_score) + (rate_weighted * rate_score);
        let timeout_ms = self.config.max_timeout_ms as f64 * backoff_score;
        let timeout_ms = timeout_ms.round() as u64;

        println!("\n| suffix_fill_score = {suffix_fill_score} | suffix_fill_weighted = {suffix_fill_weighted} \n| rate_score = {rate_score} | rate_weighted = {rate_weighted} \n| backoff_score = {backoff_score} | timeout_ms = {timeout_ms} ");
        timeout_ms
    }
}

#[cfg(test)]
mod tests {
    use time::OffsetDateTime;

    use super::{TalosBackPressureConfig, TalosBackPressureController};

    #[test]
    fn test_compute_backpressure_timeout_suffix_and_rate_below_threshold() {
        let config = TalosBackPressureConfig::new(Some(50), Some(300), Some(0.7), Some(0.5), Some(100.0));
        let mut sbp = TalosBackPressureController::with_config(config);

        let test_start_time_ns = OffsetDateTime::now_utc().unix_timestamp_nanos();

        sbp.last_suffix_head.update(0, test_start_time_ns - 30 * 1_000_000_000 /* 30 seconds*/);

        sbp.first_candidate_version.update(0, test_start_time_ns - 30 * 1_000_000_000 /* 30 seconds*/);
        sbp.last_candidate_version.update(200, test_start_time_ns - 10 * 1_000_000_000 /* 10 seconds*/);

        let timeout_ms = sbp.compute_backpressure_timeout(80, 30);

        assert_eq!(timeout_ms, 0);
    }

    #[test]
    fn test_compute_backpressure_timeout_suffix_above_and_rate_below_threshold() {
        let config = TalosBackPressureConfig::new(Some(50), Some(300), Some(0.7), Some(0.5), Some(100.0));
        let mut sbp = TalosBackPressureController::with_config(config);

        let test_start_time_ns = OffsetDateTime::now_utc().unix_timestamp_nanos();

        sbp.last_suffix_head.update(0, test_start_time_ns - 30 * 1_000_000_000 /* 30 seconds*/);

        sbp.first_candidate_version.update(0, test_start_time_ns - 30 * 1_000_000_000 /* 30 seconds*/);
        sbp.last_candidate_version.update(200, test_start_time_ns - 10 * 1_000_000_000 /* 10 seconds*/);

        let timeout_ms = sbp.compute_backpressure_timeout(80, 220);

        assert_eq!(timeout_ms, 20);
    }

    //     #[test]
    //     fn test_suffix_prune_pass() {
    //         let mut sbp = SuffixBackPressure::with_config(SuffixBackPressureConfig {
    //             size_threshold: 2_000_u64,
    //         });

    //         // initial `last_head` is 0 and new one is 20. Hence we assume prune is happening.
    //         assert!(sbp.is_prune_happening(20));
    //         assert_eq!(sbp.last_head, 20_u64);
    //         //
    //         assert!(sbp.is_prune_happening(200));
    //         // Due to any reason the suffix was blown away and we start over, and hence the new head could be less than the last_head. But this should still return true.
    //         assert!(sbp.is_prune_happening(10));
    //     }
    //     #[test]
    //     fn test_suffix_prune_fail() {
    //         let mut sbp = SuffixBackPressure::with_config(SuffixBackPressureConfig {
    //             size_threshold: 2_000_u64,
    //         });

    //         // initial `last_head` is 0 and new one is 20. Hence we assume prune is happening.
    //         assert!(sbp.is_prune_happening(20));
    //         assert_eq!(sbp.last_head, 20_u64);
    //         //
    //         assert!(sbp.is_prune_happening(3_090));
    //         // If the head is not moving, and we pass the same head as the last_head, `is_prune_happening` should return false.
    //         assert!(!sbp.is_prune_happening(3_090));
    //     }

    //     #[test]
    //     fn test_suffix_size_below_threshold() {
    //         let sbp = SuffixBackPressure::with_config(SuffixBackPressureConfig {
    //             size_threshold: 2_000_u64,
    //         });

    //         assert!(sbp.is_below_safe_threshold_using_callback(|| 100));
    //         assert!(sbp.is_below_safe_threshold_using_callback(|| 1_000));
    //     }
    //     #[test]
    //     fn test_suffix_size_above_threshold() {
    //         let sbp = SuffixBackPressure::with_config(SuffixBackPressureConfig {
    //             size_threshold: 2_000_u64,
    //         });

    //         assert!(sbp.is_below_safe_threshold_using_callback(|| 100));
    //         assert!(sbp.is_below_safe_threshold_using_callback(|| 1_000));
    //     }
}
