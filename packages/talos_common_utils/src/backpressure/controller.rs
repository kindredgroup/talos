use std::ops::Sub;

use time::OffsetDateTime;

use super::config::TalosBackPressureConfig;

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
    fn get_suffix_fill_ratio(&self, current_suffix_len: u64) -> f64 {
        // Determine the ratio of how much of suffix is filled by checking the current size againt the `TalosBackPressureConfig.suffix_max_length`
        let suffix_fill_ratio = current_suffix_len as f64 / self.config.suffix_max_length as f64;
        // Clamp it between 0.0 and 1.0. i.e Empty to full.
        suffix_fill_ratio.clamp(0.0, 1.0)
    }

    /// Computation for reactively applying backpressure based on rate gap between input and output rate.
    pub(crate) fn compute_suffix_fill_score(&self, suffix_fill_ratio: f64) -> f64 {
        // The threshold beyond which the backpressure based on suffix filling should be applied.
        let suffix_fill_weight_threshold = self.config.suffix_fill_threshold;

        // Map how much is filled over the threshold on a scale of 0-1.
        let score_to_threshold = suffix_fill_ratio.sub(suffix_fill_weight_threshold).max(0.0);
        let suffix_fill_score = score_to_threshold / (1.0 - suffix_fill_weight_threshold);

        println!(
            "
            | fill_ratio ={suffix_fill_ratio} | fill_ratio_clamped = {suffix_fill_ratio}
            | suffix_fill_weight_threshold ={suffix_fill_weight_threshold} | suffix_fill_score = {suffix_fill_score}
            "
        );
        suffix_fill_score
    }

    /// Computation for proactively applying backpressure based on rate gap between input and output rate.
    pub(crate) fn compute_rate_score(&self, current_head: u64, current_time_ns: i128) -> f64 {
        // The delta threshold between the input rate and output rate. If the rate goes beyond this threshold, we need to apply backpressure based on rate (proactive)
        let tps_threshold = self.config.rate_delta_threshold;

        // Compute the input rate based on the first and last candidate during a window
        let input_time_sec = (self.last_candidate_version.time_ns - self.first_candidate_version.time_ns) as f64 / 1_000_000_000_f64;
        let input_rate = (self.last_candidate_version.version - self.first_candidate_version.version) as f64 / input_time_sec;

        // Compute the output rate. The rate at which the head is moving helps to determine the output rate.
        let output_time_sec = (current_time_ns - self.last_suffix_head.time_ns) as f64 / 1_000_000_000_f64;
        let output_rate = (current_head - self.last_suffix_head.version) as f64 / output_time_sec;

        let delta_rate = input_rate.sub(output_rate);

        let rate_score = match tps_threshold {
            Some(threshold) if delta_rate >= threshold => {
                let log_base = threshold.log10();
                let scaled = delta_rate.log10() - log_base;
                println!(
                    "Rate score calculation => threshold_log_base = {log_base} | rate_diff_log_base = {} | scaled = {scaled} ",
                    delta_rate.log10()
                );
                scaled.clamp(0.0, 1.0)
            }
            _ => 0.0,
        };
        println!(
            "Input rate = {input_rate} | Output rate = {output_rate} | delta_rate = {delta_rate} | tps_threshold = {tps_threshold:?} | rate_score = {rate_score}"
        );

        rate_score
    }

    /// Compute the backpressure time in ms
    pub fn compute_backpressure_timeout(&self, current_head: u64, current_suffix_len: u64) -> u64 {
        let current_time_ns = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let suffix_fill_ratio = self.get_suffix_fill_ratio(current_suffix_len);

        let suffix_fill_score = self.compute_suffix_fill_score(suffix_fill_ratio);
        let rate_score = if suffix_fill_ratio > self.config.suffix_rate_threshold {
            self.compute_rate_score(current_head, current_time_ns)
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

        println!("\n| suffix_fill_score = {suffix_fill_score} | rate_score = {rate_score} \n| timeout_ms = {timeout_ms} ms");

        timeout_ms
    }
}

#[cfg(test)]
mod tests {
    use super::{TalosBackPressureConfig, TalosBackPressureController};
    use time::OffsetDateTime;

    // #[test]
    // fn test_compute_backpressure_timeout_suffix_and_rate_below_threshold() {
    //     let config = TalosBackPressureConfig::new(Some(50), Some(300), Some(0.7), Some(0.5), Some(100.0));
    //     let mut sbp = TalosBackPressureController::with_config(config);

    //     let test_start_time_ns = OffsetDateTime::now_utc().unix_timestamp_nanos();

    //     sbp.last_suffix_head.update(0, test_start_time_ns - 30 * 1_000_000_000 /* 30 seconds*/);

    //     sbp.first_candidate_version.update(0, test_start_time_ns - 30 * 1_000_000_000 /* 30 seconds*/);
    //     sbp.last_candidate_version.update(200, test_start_time_ns - 10 * 1_000_000_000 /* 10 seconds*/);

    //     let timeout_ms = sbp.compute_backpressure_timeout(80, 30);

    //     assert_eq!(timeout_ms, 0);
    // }

    #[test]
    fn test_compute_backpressure_timeout_suffix_above_and_rate_below_threshold() {
        // let config = TalosBackPressureConfig::new(Some(100), Some(300), Some(0.7), Some(0.5), Some(100.0));
        let config = TalosBackPressureConfig::builder()
            .max_timeout_ms(100)
            .min_timeout_ms(10)
            .rate_delta_threshold(100.0)
            .suffix_fill_threshold(0.7)
            .suffix_rate_threshold(0.5)
            .suffix_max_length(300)
            .build();
        let mut sbp = TalosBackPressureController::with_config(config);

        let test_start_time_ns = OffsetDateTime::now_utc().unix_timestamp_nanos();

        sbp.last_suffix_head.update(0, test_start_time_ns - 3000 * 1_000_000_000 /* 30 seconds*/);

        sbp.first_candidate_version.update(0, test_start_time_ns - 30 * 1_000_000_000 /* 30 seconds*/);
        sbp.last_candidate_version.update(3000, test_start_time_ns - 10 * 1_000_000_000 /* 10 seconds*/);

        let timeout_ms = sbp.compute_backpressure_timeout(2, 236);

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
