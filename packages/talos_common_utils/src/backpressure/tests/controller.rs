use crate::backpressure::{
    config::BackPressureConfig,
    controller::{BackPressureController, BackPressureTimeout, BackPressureVersionTracker},
};

use time::OffsetDateTime;

fn diff_seconds(time_ns: i128, diff: i128) -> i128 {
    time_ns - (diff * 1_000_000_000)
}

#[test]
fn test_backpressure_when_version_trackers_not_set() {
    let config = BackPressureConfig::builder()
        .max_timeout_ms(100)
        .min_timeout_ms(10)
        .rate_delta_threshold(100.0)
        .suffix_fill_threshold(0.7)
        .suffix_rate_threshold(0.5)
        .suffix_max_size(300)
        .build();
    let bp_controller = BackPressureController::with_config(config);

    let timeout_ms = bp_controller.calculate_timeout(20);

    assert_eq!(timeout_ms, 0);
}
#[test]
fn test_backpressure_within_threshold_limit() {
    let config = BackPressureConfig::builder()
        .max_timeout_ms(100)
        .min_timeout_ms(10)
        .rate_delta_threshold(100.0)
        .suffix_fill_threshold(0.7)
        .suffix_rate_threshold(0.5)
        .suffix_max_size(300)
        .build();
    let mut bp_controller = BackPressureController::with_config(config);

    let test_start_time_ns = OffsetDateTime::now_utc().unix_timestamp_nanos();

    // First versions
    bp_controller.update_suffix_head_trackers(BackPressureVersionTracker {
        version: 1,
        time_ns: diff_seconds(test_start_time_ns, 30),
    });
    bp_controller.update_candidate_received_tracker(BackPressureVersionTracker {
        version: 1,
        time_ns: diff_seconds(test_start_time_ns, 30),
    });

    // Last versions
    bp_controller.update_suffix_head_trackers(BackPressureVersionTracker {
        version: 10,
        time_ns: diff_seconds(test_start_time_ns, 5),
    });
    bp_controller.update_candidate_received_tracker(BackPressureVersionTracker {
        version: 10,
        time_ns: diff_seconds(test_start_time_ns, 5),
    });

    let timeout_ms = bp_controller.calculate_timeout(20);

    assert_eq!(timeout_ms, 0);
}

#[test]
/// Test backpressure timeout based on how the suffix is growing and nearing the max_suffix_size configured.
fn test_backpressure_suffix_size_based() {
    let config = BackPressureConfig::builder()
        .max_timeout_ms(100)
        .min_timeout_ms(10)
        .rate_delta_threshold(100.0)
        .suffix_fill_threshold(0.7)
        .suffix_rate_threshold(0.5)
        .suffix_max_size(300)
        .build();
    let bp_controller = BackPressureController::with_config(config);

    let timeout_ms = bp_controller.calculate_timeout(20);
    assert_eq!(timeout_ms, 0);
    // suffix_fill_threshold is 0.7, therefore at 70% threshold of suffix_max_length = 300 is 210.
    // min_timeout_ms is 10.
    let timeout_ms = bp_controller.calculate_timeout(210);
    assert_eq!(timeout_ms, 0);

    // suffix_fill_threshold is 0.7, therefore at 71% threshold of suffix_max_length = 300 is 213.
    // min_timeout_ms is 10., so the timeout_ms should be around 11ms, considering the timeout is only coming from the suffix fill size
    let timeout_ms = bp_controller.calculate_timeout(213);
    assert_eq!(timeout_ms, 11);
    // At 75% threshold of suffix_max_length = 300 is 225.
    // min_timeout_ms is 10., so the timeout_ms should be around 16ms, considering the timeout is only coming from the suffix fill size
    let timeout_ms = bp_controller.calculate_timeout(225);
    assert_eq!(timeout_ms, 16);
    // At 80% threshold of suffix_max_length = 300 is 240.
    // min_timeout_ms is 10., so the timeout_ms should be around 27ms
    let timeout_ms = bp_controller.calculate_timeout(240);
    assert_eq!(timeout_ms, 27);
    // At 85% threshold of suffix_max_length = 300 is 255.
    // min_timeout_ms is 10., so the timeout_ms should be around 42ms
    let timeout_ms = bp_controller.calculate_timeout(255);
    assert_eq!(timeout_ms, 42);
    // At 90% threshold of suffix_max_length = 300 is 270.
    // min_timeout_ms is 10., so the timeout_ms should be around 59ms
    let timeout_ms = bp_controller.calculate_timeout(270);
    assert_eq!(timeout_ms, 59);
    // At 95% threshold of suffix_max_length = 300 is 285.
    // min_timeout_ms is 10., so the timeout_ms should be around 79ms
    let timeout_ms = bp_controller.calculate_timeout(285);
    assert_eq!(timeout_ms, 78);
    // At 98% threshold of suffix_max_length = 300 is 294.
    // min_timeout_ms is 10., so the timeout_ms should be around 59ms
    let timeout_ms = bp_controller.calculate_timeout(294);
    assert_eq!(timeout_ms, 91);
    // At 100% threshold of suffix_max_length, max_timeout_ms is applied.
    let timeout_ms = bp_controller.calculate_timeout(300);
    assert_eq!(timeout_ms, 100);
    // At >100% threshold of suffix_max_length, max_timeout_ms is applied.
    let timeout_ms = bp_controller.calculate_timeout(300);
    assert_eq!(timeout_ms, 100);
}

#[test]
/// Test backpressure for various threshold of suffix
fn test_backpressure_delta_rate_based() {
    let config = BackPressureConfig::builder()
        .max_timeout_ms(100)
        .min_timeout_ms(10)
        .suffix_fill_threshold(0.7)
        .suffix_max_size(300)
        // When 50% of suffix_max_size has reached, we consider the rate_score for threshold.
        .suffix_rate_threshold(0.5)
        // When diff between input rate to output rate is more than 50, we consider the rate_score for threshold.
        .rate_delta_threshold(50.0)
        .build();
    let mut bp_controller = BackPressureController::with_config(config);

    let test_start_time_ns = OffsetDateTime::now_utc().unix_timestamp_nanos();

    let current_suffix_size_50pc = 150;
    let timeout_ms = bp_controller.calculate_timeout(current_suffix_size_50pc);
    assert_eq!(timeout_ms, 0);
    // First versions
    bp_controller.update_suffix_head_trackers(BackPressureVersionTracker {
        version: 1,
        time_ns: diff_seconds(test_start_time_ns, 300),
    });
    bp_controller.update_candidate_received_tracker(BackPressureVersionTracker {
        version: 1,
        time_ns: diff_seconds(test_start_time_ns, 300),
    });

    // Check after just the initial suffix_head and candidate_received was recorded.
    let timeout_ms = bp_controller.calculate_timeout(current_suffix_size_50pc);
    //timout_ms will be 0 delta_rate is 0 although we have crossed the 50% suffix_size.
    assert_eq!(timeout_ms, 0);

    // Next check - input is growing at > 50 tps while output is at 0.5 tps, therefore delta_rate is = 50tps
    bp_controller.update_suffix_head_trackers(BackPressureVersionTracker {
        version: 11,
        time_ns: diff_seconds(test_start_time_ns, 280),
    });
    bp_controller.update_candidate_received_tracker(BackPressureVersionTracker {
        version: 1001,
        time_ns: diff_seconds(test_start_time_ns, 280),
    });

    let timeout_ms = bp_controller.calculate_timeout(current_suffix_size_50pc);
    // At 50% suffix size/fill related score is not applied, but only rate related
    // Also at exact 50% the backpressure will not be applied
    assert_eq!(timeout_ms, 0);

    // Check delta_rate is 51tps - input_rate = 52tps while output = 1tps, therefore delta_rate is = 51tps
    bp_controller.update_suffix_head_trackers(BackPressureVersionTracker {
        version: 21,
        time_ns: diff_seconds(test_start_time_ns, 280),
    });
    bp_controller.update_candidate_received_tracker(BackPressureVersionTracker {
        version: 1041,
        time_ns: diff_seconds(test_start_time_ns, 280),
    });

    let timeout_ms = bp_controller.calculate_timeout(current_suffix_size_50pc);
    // At 51% suffix size/fill related score is not applied.
    // Only backpressure from rates is applied. From the delta_rate, min and max timeout, we can calculate the timeout to be 11ms.
    assert_eq!(timeout_ms, 11);

    // Check delta_rate is 55tps - input_rate = 56tps while output = 1tps, therefore delta_rate is = 55tps
    bp_controller.update_suffix_head_trackers(BackPressureVersionTracker {
        version: 21,
        time_ns: diff_seconds(test_start_time_ns, 280),
    });
    bp_controller.update_candidate_received_tracker(BackPressureVersionTracker {
        version: 1121,
        time_ns: diff_seconds(test_start_time_ns, 280),
    });

    let timeout_ms = bp_controller.calculate_timeout(current_suffix_size_50pc);
    // At 55% suffix size/fill related score is not applied.
    // Only backpressure from rates is applied. From the delta_rate, min and max timeout, we can calculate the timeout to be 14ms.
    assert_eq!(timeout_ms, 14);

    // Check delta_rate is 65tps - input_rate = 66tps while output = 1tps, therefore delta_rate is = 65tps
    bp_controller.update_suffix_head_trackers(BackPressureVersionTracker {
        version: 21,
        time_ns: diff_seconds(test_start_time_ns, 280),
    });
    bp_controller.update_candidate_received_tracker(BackPressureVersionTracker {
        version: 1321,
        time_ns: diff_seconds(test_start_time_ns, 280),
    });

    let timeout_ms = bp_controller.calculate_timeout(current_suffix_size_50pc);
    // At 50% suffix size/fill related score is not applied.
    // Only backpressure from rates is applied. From the delta_rate, min and max timeout, we can calculate the timeout to be 20ms.
    assert_eq!(timeout_ms, 20);

    // Check delta_rate is 70tps - input_rate = 71tps while output = 1tps, therefore delta_rate is = 70tps
    bp_controller.update_suffix_head_trackers(BackPressureVersionTracker {
        version: 21,
        time_ns: diff_seconds(test_start_time_ns, 280),
    });
    bp_controller.update_candidate_received_tracker(BackPressureVersionTracker {
        version: 1421,
        time_ns: diff_seconds(test_start_time_ns, 280),
    });

    let timeout_ms = bp_controller.calculate_timeout(current_suffix_size_50pc);
    // At 50% suffix size/fill related score is not applied.
    // Only backpressure from rates is applied. From the delta_rate, min and max timeout, we can calculate the timeout to be 23ms.
    assert_eq!(timeout_ms, 23);

    // Same as previous test, but suffix size is 71. i.e it is above the suffix_fill_threshold, and therefore that also plays a part in the timeout calculation.
    // Check delta_rate is 70tps - input_rate = 72tps while output = 1tps, therefore delta_rate is = 70tps
    bp_controller.update_suffix_head_trackers(BackPressureVersionTracker {
        version: 21,
        time_ns: diff_seconds(test_start_time_ns, 280),
    });
    bp_controller.update_candidate_received_tracker(BackPressureVersionTracker {
        version: 1421,
        time_ns: diff_seconds(test_start_time_ns, 280),
    });

    let current_suffix_size_71pc = 213;
    let timeout_ms = bp_controller.calculate_timeout(current_suffix_size_71pc);
    // At 71% suffix size/fill related score and delta_rate related scores will be applied
    // Therefore combined timeout = min + ((max-min) * (fill_weighted + (rate_weighted * rate_score))
    // ==>                        = 10 + (90 * ( 0.006085806195 + ( 0.999 * 0.0.146)  ) )
    assert_eq!(timeout_ms, 24);

    // Check delta_rate is 300tps - input_rate = 301tps while output = 1tps, therefore delta_rate is = 65tps
    bp_controller.update_suffix_head_trackers(BackPressureVersionTracker {
        version: 21,
        time_ns: diff_seconds(test_start_time_ns, 280),
    });
    bp_controller.update_candidate_received_tracker(BackPressureVersionTracker {
        version: 6021,
        time_ns: diff_seconds(test_start_time_ns, 280),
    });

    let timeout_ms = bp_controller.calculate_timeout(current_suffix_size_50pc);
    // At 50% suffix size/fill related score is not applied.
    // Only backpressure from rates is applied. From the delta_rate, min and max timeout, we can calculate the timeout to be 80ms.
    assert_eq!(timeout_ms, 80);
}

#[test]
fn test_backpressure_timeouts_no_timeout_scenarios() {
    let config = BackPressureConfig::builder()
        .max_timeout_ms(100)
        .min_timeout_ms(10)
        .suffix_fill_threshold(0.7)
        .suffix_max_size(300)
        // When 50% of suffix_max_size has reached, we consider the rate_score for threshold.
        .suffix_rate_threshold(0.5)
        // When diff between input rate to output rate is more than 50, we consider the rate_score for threshold.
        .rate_delta_threshold(50.0)
        .build();
    let mut bp_controller = BackPressureController::with_config(config);

    let test_start_time_ns = OffsetDateTime::now_utc().unix_timestamp_nanos();

    // ****************************************************************************************
    //                              NO TIMEOUT SCENARIOS
    // ****************************************************************************************
    // AS01. Since this is initial call, with the head and inputs not tracked, there should be no timeout.
    let timeout = bp_controller.compute_backpressure(20);
    assert_eq!(timeout, BackPressureTimeout::NoTimeout);

    // AS02. When rate is very high, but suffix and rate thresholds haven't crossed. There should be no timeout.
    bp_controller.update_suffix_head_trackers(BackPressureVersionTracker::new(10, diff_seconds(test_start_time_ns, 200)));
    bp_controller.update_candidate_received_tracker(BackPressureVersionTracker::new(10, diff_seconds(test_start_time_ns, 200)));

    bp_controller.update_suffix_head_trackers(BackPressureVersionTracker::new(20, diff_seconds(test_start_time_ns, 100)));
    bp_controller.update_candidate_received_tracker(BackPressureVersionTracker::new(10_000, diff_seconds(test_start_time_ns, 100)));
    let timeout = bp_controller.compute_backpressure(20);
    assert_eq!(timeout, BackPressureTimeout::NoTimeout);

    // AS02. When suffix is above the rate threshold but below the suffix threshold,
    //       but rate is below the delta rate threshold.
    //       There should be no timeout.
    // From below trackers, the delta_rate is around 80tps, this is below the required `delta_rate_threshold`.
    bp_controller.update_suffix_head_trackers(BackPressureVersionTracker::new(
        1,
        diff_seconds(test_start_time_ns, 2 * 1_000_000_000 /*2 seconds */),
    ));
    bp_controller.update_candidate_received_tracker(BackPressureVersionTracker::new(1, diff_seconds(test_start_time_ns, 2 * 1_000_000_000)));

    bp_controller.update_suffix_head_trackers(BackPressureVersionTracker::new(
        921,
        diff_seconds(test_start_time_ns, 1_000_000_000 /*2 seconds */),
    ));
    bp_controller.update_candidate_received_tracker(BackPressureVersionTracker::new(
        1_001,
        diff_seconds(test_start_time_ns, 1_000_000_000 /*2 seconds */),
    ));
    // 60% of max_suffix_size. This is above the threshold of rate and suffix fill score
    let current_suffix_size = 180;
    let timeout = bp_controller.compute_backpressure(current_suffix_size);
    assert_eq!(timeout, BackPressureTimeout::NoTimeout);
}

#[test]
fn test_backpressure_timeouts_valid_timeout_scenarios() {
    let config = BackPressureConfig::builder()
        .max_timeout_ms(100)
        .min_timeout_ms(10)
        .suffix_fill_threshold(0.7)
        .suffix_max_size(300)
        // When 50% of suffix_max_size has reached, we consider the rate_score for threshold.
        .suffix_rate_threshold(0.5)
        // When diff between input rate to output rate is more than 50, we consider the rate_score for threshold.
        .rate_delta_threshold(50.0)
        // Override the default of 30ms to 2 seconds.
        .max_head_stale_timeout_ms(2 * 1_000)
        .build();
    let mut bp_controller = BackPressureController::with_config(config);

    let test_start_time_ns = OffsetDateTime::now_utc().unix_timestamp_nanos();

    // ****************************************************************************************
    //                               TIMEOUT SCENARIOS
    // ****************************************************************************************
    // AS01. Valid timeout when rate is low, but the suffix size is above threshold.
    // 80% of max_suffix_size. This is above the threshold `suffix_fill_threshold
    let current_suffix_size = 240;
    let timeout = bp_controller.compute_backpressure(current_suffix_size);
    assert!(matches!(timeout, BackPressureTimeout::Timeout(..)));

    // AS02. Valid timeout when rate is high and suffix size is above the `suffix_rate_threshold`, but below the `suffix_fill_threshold`.

    // From below trackers, the delta_rate is around 70tps, which is above the `delta_rate_threshold`
    bp_controller.update_suffix_head_trackers(BackPressureVersionTracker::new(1, diff_seconds(test_start_time_ns, 300)));
    bp_controller.update_candidate_received_tracker(BackPressureVersionTracker::new(1, diff_seconds(test_start_time_ns, 300)));

    bp_controller.update_suffix_head_trackers(BackPressureVersionTracker::new(21, diff_seconds(test_start_time_ns, 280 /*2 seconds */)));
    bp_controller.update_candidate_received_tracker(BackPressureVersionTracker::new(1421, diff_seconds(test_start_time_ns, 280 /*2 seconds */)));
    // 60% of max_suffix_size. This is above the threshold of rate but below the threshold for suffix fill score
    let current_suffix_size = 180;
    let timeout = bp_controller.compute_backpressure(current_suffix_size);
    assert!(matches!(timeout, BackPressureTimeout::Timeout(..)));

    // AS03. Valid timeout when rate is high and suffix size is above the `suffix_rate_threshold` and the `suffix_fill_threshold`.
    // From below trackers, the delta_rate is around 70tps, which is above the `delta_rate_threshold`
    bp_controller.update_suffix_head_trackers(BackPressureVersionTracker::new(1, diff_seconds(test_start_time_ns, 300)));
    bp_controller.update_candidate_received_tracker(BackPressureVersionTracker::new(1, diff_seconds(test_start_time_ns, 300)));

    bp_controller.update_suffix_head_trackers(BackPressureVersionTracker::new(21, diff_seconds(test_start_time_ns, 280 /*2 seconds */)));
    bp_controller.update_candidate_received_tracker(BackPressureVersionTracker::new(1421, diff_seconds(test_start_time_ns, 280 /*2 seconds */)));
    // 96% of max_suffix_size. This is above the threshold rate but below the threshold for suffix fill score
    let current_suffix_size = 290;
    let timeout = bp_controller.compute_backpressure(current_suffix_size);
    assert!(matches!(timeout, BackPressureTimeout::Timeout(..)));
}

#[test]
fn test_backpressure_timeouts_critical_timeout_scenarios() {
    let config = BackPressureConfig::builder()
        .max_timeout_ms(100)
        .min_timeout_ms(10)
        .suffix_fill_threshold(0.7)
        .suffix_max_size(300)
        // When 50% of suffix_max_size has reached, we consider the rate_score for threshold.
        .suffix_rate_threshold(0.5)
        // When diff between input rate to output rate is more than 50, we consider the rate_score for threshold.
        .rate_delta_threshold(50.0)
        // Override the default of 30ms to 1 ms.
        .max_head_stale_timeout_ms(1)
        .build();
    let mut bp_controller = BackPressureController::with_config(config);

    let test_start_time_ns = OffsetDateTime::now_utc().unix_timestamp_nanos();

    // ****************************************************************************************
    //                               CRITICAL SCENARIOS
    // ****************************************************************************************
    // AS04. Critical when the suffix head has been stale for too long
    // From below trackers, the delta_rate is around 70tps, which is above the `delta_rate_threshold`
    bp_controller.update_suffix_head_trackers(BackPressureVersionTracker::new(1, diff_seconds(test_start_time_ns, 300)));
    bp_controller.update_candidate_received_tracker(BackPressureVersionTracker::new(1, diff_seconds(test_start_time_ns, 300)));

    bp_controller.update_suffix_head_trackers(BackPressureVersionTracker::new(21, diff_seconds(test_start_time_ns, 280)));
    bp_controller.update_candidate_received_tracker(BackPressureVersionTracker::new(1421, diff_seconds(test_start_time_ns, 280)));
    // 100% of max_suffix_size. This is above the threshold rate but below the threshold for suffix fill score
    let current_suffix_size = 300;
    let timeout = bp_controller.compute_backpressure(current_suffix_size);
    assert!(matches!(timeout, BackPressureTimeout::CriticalStop(..)));
}
