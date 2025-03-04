use std::sync::Arc;

use opentelemetry::{
    global,
    metrics::{Counter, Histogram},
};

pub struct CohortMeters {
    oo_retry_counter: Arc<Counter<u64>>,
    oo_giveups_counter: Arc<Counter<u64>>,
    oo_not_safe_counter: Arc<Counter<u64>>,
    oo_install_histogram: Arc<Histogram<f64>>,
    oo_attempts_histogram: Arc<Histogram<u64>>,
    oo_install_and_wait_histogram: Arc<Histogram<f64>>,
    oo_wait_histogram: Arc<Histogram<f64>>,
    talos_histogram: Arc<Histogram<f64>>,
    talos_aborts_counter: Arc<Counter<u64>>,
    agent_retries_histogram: Arc<Histogram<u64>>,
    agent_errors_counter: Arc<Counter<u64>>,
    db_errors_counter: Arc<Counter<u64>>,
}

impl Default for CohortMeters {
    fn default() -> Self {
        Self::new()
    }
}

impl CohortMeters {
    pub fn new() -> CohortMeters {
        let meter = global::meter("cohort_sdk");
        let oo_install_histogram = meter.f64_histogram("metric_oo_install_duration").with_unit("ms").build();
        let oo_attempts_histogram = meter.u64_histogram("metric_oo_attempts").with_unit("tx").build();
        let oo_install_and_wait_histogram = meter.f64_histogram("metric_oo_install_and_wait_duration").with_unit("ms").build();
        let oo_wait_histogram = meter.f64_histogram("metric_oo_wait_duration").with_unit("ms").build();
        let talos_histogram = meter.f64_histogram("metric_talos").with_unit("ms").build();
        let oo_retry_counter = meter.u64_counter("metric_oo_retry_count").with_unit("tx").build();
        let oo_giveups_counter = meter.u64_counter("metric_oo_giveups_count").with_unit("tx").build();
        let oo_not_safe_counter = meter.u64_counter("metric_oo_not_safe_count").with_unit("tx").build();
        let talos_aborts_counter = meter.u64_counter("metric_talos_aborts_count").with_unit("tx").build();
        let agent_errors_counter = meter.u64_counter("metric_agent_errors_count").with_unit("tx").build();
        let agent_retries_histogram = meter.u64_histogram("metric_agent_retries").with_unit("tx").build();
        let db_errors_counter = meter.u64_counter("metric_db_errors_count").with_unit("tx").build();

        oo_retry_counter.add(0, &[]);
        oo_giveups_counter.add(0, &[]);
        oo_not_safe_counter.add(0, &[]);
        talos_aborts_counter.add(0, &[]);
        agent_errors_counter.add(0, &[]);
        db_errors_counter.add(0, &[]);

        Self {
            oo_install_histogram: Arc::new(oo_install_histogram),
            oo_install_and_wait_histogram: Arc::new(oo_install_and_wait_histogram),
            oo_wait_histogram: Arc::new(oo_wait_histogram),
            oo_retry_counter: Arc::new(oo_retry_counter),
            oo_giveups_counter: Arc::new(oo_giveups_counter),
            oo_not_safe_counter: Arc::new(oo_not_safe_counter),
            oo_attempts_histogram: Arc::new(oo_attempts_histogram),
            talos_histogram: Arc::new(talos_histogram),
            agent_retries_histogram: Arc::new(agent_retries_histogram),
            talos_aborts_counter: Arc::new(talos_aborts_counter),
            agent_errors_counter: Arc::new(agent_errors_counter),
            db_errors_counter: Arc::new(db_errors_counter),
        }
    }

    pub fn update_talos_metric(&self, value: f64) {
        let hist = Arc::clone(&self.talos_histogram);
        tokio::spawn(async move {
            hist.record(value, &[]);
        });
    }

    pub fn update_oo_install_metric(&self, value: f64) {
        let hist = Arc::clone(&self.oo_install_histogram);
        tokio::spawn(async move {
            hist.record(value, &[]);
        });
    }

    pub fn update_post_oo_install_metrics(
        &self,
        number_of_not_save_responses: u64,
        total_sleep: u128,
        number_of_giveups: u64,
        number_of_attempts: u32,
        duration: f64,
    ) {
        let c_not_safe = Arc::clone(&self.oo_not_safe_counter);
        let h_total_sleep = Arc::clone(&self.oo_wait_histogram);
        let h_attempts = Arc::clone(&self.oo_attempts_histogram);
        let h_span_2 = Arc::clone(&self.oo_install_and_wait_histogram);
        let c_giveups = Arc::clone(&self.oo_giveups_counter);
        let c_retry = Arc::clone(&self.oo_retry_counter);

        tokio::spawn(async move {
            if number_of_not_save_responses > 0 {
                c_not_safe.add(number_of_not_save_responses, &[]);
            }
            if total_sleep > 0 {
                h_total_sleep.record(total_sleep as f64 * 100.0, &[]);
            }
            if number_of_giveups > 0 {
                c_giveups.add(number_of_giveups, &[]);
            }
            if number_of_attempts > 1 {
                c_retry.add(number_of_attempts as u64 - 1, &[]);
            }

            h_attempts.record(number_of_attempts as u64, &[]);
            h_span_2.record(duration * 100.0, &[]);
        });
    }

    pub fn update_post_send_to_talos_metrics(&self, agent_errors: u64, db_errors: u64, talos_aborts: u64, attempts: u32) {
        let c_talos_aborts = Arc::clone(&self.talos_aborts_counter);
        let c_agent_errors = Arc::clone(&self.agent_errors_counter);
        let c_db_errors = Arc::clone(&self.db_errors_counter);
        let h_agent_retries = Arc::clone(&self.agent_retries_histogram);

        if agent_errors > 0 || db_errors > 0 || talos_aborts > 0 || attempts > 0 {
            tokio::spawn(async move {
                c_talos_aborts.add(talos_aborts, &[]);
                c_agent_errors.add(agent_errors, &[]);
                c_db_errors.add(db_errors, &[]);
                h_agent_retries.record(attempts as u64, &[]);
            });
        }
    }
}
