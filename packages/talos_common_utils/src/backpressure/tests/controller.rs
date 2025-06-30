use crate::backpressure::{
    config::BackPressureConfig,
    controller::{BackPressureController, BackPressureVersionTracker},
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
    let mut bp_controller = BackPressureController::with_config(config);

    let timeout_ms = bp_controller.get_timeout_ms(20);

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

    let timeout_ms = bp_controller.get_timeout_ms(20);

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

    let timeout_ms = bp_controller.compute_backpressure_timeout(20);
    assert_eq!(timeout_ms, 0);
    // suffix_fill_threshold is 0.7, therefore at 70% threshold of suffix_max_length = 300 is 210.
    // min_timeout_ms is 10.
    let timeout_ms = bp_controller.compute_backpressure_timeout(210);
    assert_eq!(timeout_ms, 0);

    // suffix_fill_threshold is 0.7, therefore at 71% threshold of suffix_max_length = 300 is 213.
    // min_timeout_ms is 10., so the timeout_ms should be around 11ms, considering the timeout is only coming from the suffix fill size
    let timeout_ms = bp_controller.compute_backpressure_timeout(213);
    assert_eq!(timeout_ms, 11);
    // At 75% threshold of suffix_max_length = 300 is 225.
    // min_timeout_ms is 10., so the timeout_ms should be around 16ms, considering the timeout is only coming from the suffix fill size
    let timeout_ms = bp_controller.compute_backpressure_timeout(225);
    assert_eq!(timeout_ms, 16);
    // At 80% threshold of suffix_max_length = 300 is 240.
    // min_timeout_ms is 10., so the timeout_ms should be around 27ms
    let timeout_ms = bp_controller.compute_backpressure_timeout(240);
    assert_eq!(timeout_ms, 27);
    // At 85% threshold of suffix_max_length = 300 is 255.
    // min_timeout_ms is 10., so the timeout_ms should be around 42ms
    let timeout_ms = bp_controller.compute_backpressure_timeout(255);
    assert_eq!(timeout_ms, 42);
    // At 90% threshold of suffix_max_length = 300 is 270.
    // min_timeout_ms is 10., so the timeout_ms should be around 59ms
    let timeout_ms = bp_controller.compute_backpressure_timeout(270);
    assert_eq!(timeout_ms, 59);
    // At 95% threshold of suffix_max_length = 300 is 285.
    // min_timeout_ms is 10., so the timeout_ms should be around 79ms
    let timeout_ms = bp_controller.compute_backpressure_timeout(285);
    assert_eq!(timeout_ms, 78);
    // At 98% threshold of suffix_max_length = 300 is 294.
    // min_timeout_ms is 10., so the timeout_ms should be around 59ms
    let timeout_ms = bp_controller.compute_backpressure_timeout(294);
    assert_eq!(timeout_ms, 91);
    // At 100% threshold of suffix_max_length, max_timeout_ms is applied.
    let timeout_ms = bp_controller.compute_backpressure_timeout(300);
    assert_eq!(timeout_ms, 100);
    // At >100% threshold of suffix_max_length, max_timeout_ms is applied.
    let timeout_ms = bp_controller.compute_backpressure_timeout(300);
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
    let timeout_ms = bp_controller.compute_backpressure_timeout(current_suffix_size_50pc);
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
    let timeout_ms = bp_controller.compute_backpressure_timeout(current_suffix_size_50pc);
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

    let timeout_ms = bp_controller.compute_backpressure_timeout(current_suffix_size_50pc);
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

    let timeout_ms = bp_controller.compute_backpressure_timeout(current_suffix_size_50pc);
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

    let timeout_ms = bp_controller.compute_backpressure_timeout(current_suffix_size_50pc);
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

    let timeout_ms = bp_controller.compute_backpressure_timeout(current_suffix_size_50pc);
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

    let timeout_ms = bp_controller.compute_backpressure_timeout(current_suffix_size_50pc);
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
    let timeout_ms = bp_controller.compute_backpressure_timeout(current_suffix_size_71pc);
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

    let timeout_ms = bp_controller.compute_backpressure_timeout(current_suffix_size_50pc);
    // At 50% suffix size/fill related score is not applied.
    // Only backpressure from rates is applied. From the delta_rate, min and max timeout, we can calculate the timeout to be 80ms.
    assert_eq!(timeout_ms, 80);
}

#[test]
fn test_compute_backpressure_timeout_suffix_above_and_rate_below_threshold() {
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

    // Update the suffix head trackers
    bp_controller.update_suffix_head_trackers(BackPressureVersionTracker {
        version: 1,
        time_ns: diff_seconds(test_start_time_ns, 3_000),
    });
    bp_controller.update_suffix_head_trackers(BackPressureVersionTracker {
        version: 20,
        time_ns: diff_seconds(test_start_time_ns, 10),
    });
    // Update the candidates trackers
    bp_controller.update_candidate_received_tracker(BackPressureVersionTracker {
        version: 1,
        time_ns: diff_seconds(test_start_time_ns, 20),
    });
    bp_controller.update_candidate_received_tracker(BackPressureVersionTracker {
        version: 4005,
        time_ns: diff_seconds(test_start_time_ns, 10),
    });

    assert_eq!(bp_controller.first_suffix_head.version, 1);
    assert_eq!(bp_controller.first_candidate_received.version, 1);

    let timeout_ms = bp_controller.get_timeout_ms(236);

    // Greater than 50 ms
    assert!(timeout_ms > 50);
    assert_eq!(bp_controller.first_suffix_head.version, 0);
    assert_eq!(bp_controller.first_candidate_received.version, 0);

    // 2. Next check window -
    // We update the candidate and head first received, but no further updates for both, and the suffix length is still > the threshold.
    // Under this scenario, the because the delta_rate would not have any impact on the timeout_ms, but only the suffix_fill related part will have impact.
    //  - 236/300 is around 78%. That means we need to control for the remaining 22% from filling quickly.
    //  - min_timeout_ms is 10ms and max_timeout_ms is 100ms for our test. Therefore, the timeout needed to be applied by running through the formulaes is around 25ms
    let test_start_time_ns = OffsetDateTime::now_utc().unix_timestamp_nanos();

    bp_controller.update_suffix_head_trackers(BackPressureVersionTracker {
        version: 40,
        time_ns: diff_seconds(test_start_time_ns, 10),
    });
    // Update the candidates trackers
    bp_controller.update_candidate_received_tracker(BackPressureVersionTracker {
        version: 8000,
        time_ns: diff_seconds(test_start_time_ns, 20),
    });
    let timeout_ms = bp_controller.get_timeout_ms(236);

    // We don't need a precise value, some timeout value within the acceptable range is good for the test to pass. The calculations are very complex to write out the exact value here.
    assert!(timeout_ms > 22 && timeout_ms < 27);

    // 3. Next check window -
    // We update the candidate and head first received, then the head moves by just 10 while the incomings candidates moves a lot, creating a scenario of very high input rate, while output rate is very low, and the suffix length is still > the threshold.
    // Under this scenario, the because the delta_rate > the rate_threshold delta_rate will have an impact on the timeout_ms calculation for back pressure.
    //  - 250/300 is around 83%. That means we need to control for the remaining 17% from filling quickly.
    //  - min_timeout_ms is 10ms and max_timeout_ms is 100ms for our test.
    // fill_score = 0.296 | rate_weighted = 1 - 0.296 = 0.703 | rate_score = 0.476 | Therefore timeout_ms = 10 + ((100 - 10) * (0.296 + (0.703 * 0.476)) =~ 66.75ms  rounded to 67ms

    let test_start_time_ns = OffsetDateTime::now_utc().unix_timestamp_nanos();

    bp_controller.update_suffix_head_trackers(BackPressureVersionTracker {
        version: 50,
        time_ns: diff_seconds(test_start_time_ns, 20),
    });
    // Update the candidates trackers
    bp_controller.update_candidate_received_tracker(BackPressureVersionTracker {
        version: 9000,
        time_ns: diff_seconds(test_start_time_ns, 20),
    });

    //*** */
    bp_controller.update_suffix_head_trackers(BackPressureVersionTracker {
        version: 60,
        time_ns: diff_seconds(test_start_time_ns, 2),
    });
    // Update the candidates trackers
    bp_controller.update_candidate_received_tracker(BackPressureVersionTracker {
        version: 15000,
        time_ns: diff_seconds(test_start_time_ns, 0),
    });
    let timeout_ms = bp_controller.get_timeout_ms(250);

    // We don't need a precise value, some timeout value within the acceptable range is good for the test to pass. The calculations are very complex to write out the exact value here.
    assert!(timeout_ms > 60 && timeout_ms < 70);
}

// #[test]
// /// Test backpressure for various combination of delta_rates and suffix size
// fn test_backpressure_delta_rate_and_suffix_size_combination() {
//     // Add tests for various combinations of rate and suffix size
//     assert_eq!(0, 1); // Will fail. Just a placeholder till tests are in place
// }
