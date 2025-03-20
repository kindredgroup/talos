use opentelemetry::{
    global,
    metrics::{Counter, Histogram},
};

pub struct CohortMeters {
    enabled: bool,
    oo_retry_counter: Option<Counter<u64>>,
    oo_giveups_counter: Option<Counter<u64>>,
    oo_not_safe_counter: Option<Counter<u64>>,
    oo_install_duration_hist: Option<Histogram<f64>>,
    oo_attempts_hist: Option<Histogram<u64>>,
    oo_install_and_wait_duration_hist: Option<Histogram<f64>>,
    oo_wait_duration_hist: Option<Histogram<f64>>,
    talos_duration_hist: Option<Histogram<f64>>,
    talos_aborts_counter: Option<Counter<u64>>,
    agent_retries_hist: Option<Histogram<u64>>,
    agent_errors_counter: Option<Counter<u64>>,
    db_errors_counter: Option<Counter<u64>>,
}

impl Default for CohortMeters {
    fn default() -> Self {
        Self::new(false)
    }
}

impl CohortMeters {
    pub fn new(enabled: bool) -> CohortMeters {
        if enabled {
            let meter = global::meter("cohort_sdk");
            let oo_install_duration_hist = meter.f64_histogram("metric_oo_install").with_unit("ms").build();
            let oo_attempts_hist = meter.u64_histogram("metric_oo_attempts").with_unit("tx").build();
            let oo_install_and_wait_duration_hist = meter.f64_histogram("metric_oo_install_and_wait").with_unit("ms").build();
            let oo_wait_duration_hist = meter.f64_histogram("metric_oo_wait").with_unit("ms").build();
            let talos_duration_hist = meter.f64_histogram("metric_talos").with_unit("ms").build();
            let agent_retries_hist = meter.u64_histogram("metric_agent_retries").with_unit("tx").build();

            let oo_retry_counter = meter.u64_counter("metric_oo_retry").with_unit("tx").build();
            let oo_giveups_counter = meter.u64_counter("metric_oo_giveups").with_unit("tx").build();
            let oo_not_safe_counter = meter.u64_counter("metric_oo_not_safe").with_unit("tx").build();
            let talos_aborts_counter = meter.u64_counter("metric_talos_aborts").with_unit("tx").build();
            let agent_errors_counter = meter.u64_counter("metric_agent_errors").with_unit("tx").build();
            let db_errors_counter = meter.u64_counter("metric_db_errors").with_unit("tx").build();

            oo_retry_counter.add(0, &[]);
            oo_giveups_counter.add(0, &[]);
            oo_not_safe_counter.add(0, &[]);
            talos_aborts_counter.add(0, &[]);
            agent_errors_counter.add(0, &[]);
            db_errors_counter.add(0, &[]);

            Self {
                enabled,
                oo_install_duration_hist: Some(oo_install_duration_hist),
                oo_install_and_wait_duration_hist: Some(oo_install_and_wait_duration_hist),
                oo_wait_duration_hist: Some(oo_wait_duration_hist),
                oo_retry_counter: Some(oo_retry_counter),
                oo_giveups_counter: Some(oo_giveups_counter),
                oo_not_safe_counter: Some(oo_not_safe_counter),
                oo_attempts_hist: Some(oo_attempts_hist),
                talos_duration_hist: Some(talos_duration_hist),
                agent_retries_hist: Some(agent_retries_hist),
                talos_aborts_counter: Some(talos_aborts_counter),
                agent_errors_counter: Some(agent_errors_counter),
                db_errors_counter: Some(db_errors_counter),
            }
        } else {
            Self {
                enabled,
                oo_install_duration_hist: None,
                oo_install_and_wait_duration_hist: None,
                oo_wait_duration_hist: None,
                oo_retry_counter: None,
                oo_giveups_counter: None,
                oo_not_safe_counter: None,
                oo_attempts_hist: None,
                talos_duration_hist: None,
                agent_retries_hist: None,
                talos_aborts_counter: None,
                agent_errors_counter: None,
                db_errors_counter: None,
            }
        }
    }

    pub fn update_talos_metric(&self, value: f64) {
        if !self.enabled {
            return;
        }

        let metric = self.talos_duration_hist.clone().unwrap();
        tokio::spawn(async move {
            metric.record(value, &[]);
        });
    }

    pub fn update_oo_install_metric(&self, value: f64) {
        if !self.enabled {
            return;
        }

        let metric = self.oo_install_duration_hist.clone().unwrap();
        tokio::spawn(async move {
            metric.record(value, &[]);
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
        if !self.enabled {
            return;
        }

        let c_not_safe = self.oo_not_safe_counter.clone().unwrap();
        let h_total_sleep = self.oo_wait_duration_hist.clone().unwrap();
        let h_attempts = self.oo_attempts_hist.clone().unwrap();
        let h_span_2 = self.oo_install_and_wait_duration_hist.clone().unwrap();
        let c_giveups = self.oo_giveups_counter.clone().unwrap();
        let c_retry = self.oo_retry_counter.clone().unwrap();

        tokio::spawn(async move {
            if number_of_not_save_responses > 0 {
                c_not_safe.add(number_of_not_save_responses, &[]);
            }
            if total_sleep > 0 {
                h_total_sleep.record(total_sleep as f64, &[]);
            }
            if number_of_giveups > 0 {
                c_giveups.add(number_of_giveups, &[]);
            }
            if number_of_attempts > 1 {
                c_retry.add(number_of_attempts as u64 - 1, &[]);
            }

            h_attempts.record(number_of_attempts as u64, &[]);
            h_span_2.record(duration, &[]);
        });
    }

    pub fn update_post_send_to_talos_metrics(&self, agent_errors: u64, db_errors: u64, talos_aborts: u64, attempts: u32) {
        if !self.enabled {
            return;
        }

        let c_talos_aborts = self.talos_aborts_counter.clone().unwrap();
        let c_agent_errors = self.agent_errors_counter.clone().unwrap();
        let c_db_errors = self.db_errors_counter.clone().unwrap();
        let h_agent_retries = self.agent_retries_hist.clone().unwrap();

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
