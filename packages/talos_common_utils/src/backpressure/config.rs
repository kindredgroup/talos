#[derive(Debug)]
pub struct BackPressureConfig {
    /// The window/interval used to check if backpressure should be applied.
    /// - **Defaults to 100ms.**
    pub check_window_ms: u64,
    /// Max timeout in ms while calculating the back pressure in milliseconds.
    /// - **Defaults to 50ms.**
    pub max_timeout_ms: u64,
    /// Min timeout in ms while calculating the back pressure in milliseconds.
    /// - **Defaults to 5ms.**
    pub min_timeout_ms: u64,
    /// Max length of suffix which is considered safe to avoid memory related issues.
    /// - **Defaults to 10_000.**
    pub suffix_max_size: u64,
    /// Suffix fill threshold is used enable back pressure logic based on the current length of suffix against `suffix_max_length`.
    /// The reactive back pressure strategy will not trigger till this threshold is crossed.
    /// - threshold should be between 0.0 and 1.0.
    /// - Values beyond this range will be clamped to lower or upper bound.
    /// - **Defaults to 0.7.** i.e when current length has reached `70%` of `suffix_max_length`.
    pub suffix_fill_threshold: f64,
    /// Suffix rate threshold is used enable back pressure logic based on the difference between input vs output rate, when the suffix has crossed this threshold.
    /// The proactive back pressure strategy will not trigger till this threshold is crossed.
    /// - threshold should be between 0.0 and `suffix_fill_threshold`.
    /// - Values beyond this range will be clamped to lower or upper bound.
    /// - **Defaults to 0.5.** i.e when current length has reached `50%` of `suffix_max_length`.
    pub suffix_rate_threshold: f64,
    /// Suffix rate threshold is used enable back pressure logic based on the difference between input vs output rate.
    /// The proactive back pressure strategy will not trigger till `suffix_rate_threshold` + this rate threshold is crossed.
    /// - If Some valid value is passed., the rate based proactive back pressure logic is applied only when this threshold is crossed.
    ///         - e.g. If `rate_delta_threshold = 100`, then proactive back pressure logic kicks in when `input_tps - output_tps > 100`.
    /// - **Defaults to `None`.** i.e rate based proactive back pressure will not run
    pub rate_delta_threshold: Option<f64>,
}

impl Default for BackPressureConfig {
    fn default() -> Self {
        Self {
            check_window_ms: 100, // 100ms
            max_timeout_ms: 50,   // 50ms
            min_timeout_ms: 5,    // 50ms
            suffix_max_size: 10_000,
            suffix_fill_threshold: 0.7, // 70% of suffix
            suffix_rate_threshold: 0.5, // 50% of suffix
            rate_delta_threshold: None,
        }
    }
}

impl BackPressureConfig {
    pub fn builder() -> BackPressureConfigBuilder {
        BackPressureConfigBuilder::default()
    }
}

#[derive(Default)]
pub struct BackPressureConfigBuilder {
    check_window_ms: Option<u64>,
    max_timeout_ms: Option<u64>,
    min_timeout_ms: Option<u64>,
    suffix_max_size: Option<u64>,
    suffix_fill_threshold: Option<f64>,
    suffix_rate_threshold: Option<f64>,
    rate_delta_threshold: Option<f64>,
}

impl BackPressureConfigBuilder {
    pub fn check_window_ms(mut self, window_ms: u64) -> Self {
        self.check_window_ms = Some(window_ms);
        self
    }
    pub fn max_timeout_ms(mut self, timeout: u64) -> Self {
        self.max_timeout_ms = Some(timeout);
        self
    }

    pub fn min_timeout_ms(mut self, timeout: u64) -> Self {
        self.min_timeout_ms = Some(timeout);
        self
    }

    pub fn suffix_max_size(mut self, length: u64) -> Self {
        self.suffix_max_size = Some(length);
        self
    }

    pub fn suffix_fill_threshold(mut self, threshold: f64) -> Self {
        self.suffix_fill_threshold = Some(threshold);
        self
    }

    pub fn suffix_rate_threshold(mut self, threshold: f64) -> Self {
        self.suffix_rate_threshold = Some(threshold);
        self
    }

    pub fn rate_delta_threshold(mut self, threshold: f64) -> Self {
        self.rate_delta_threshold = Some(threshold);
        self
    }

    pub fn build(self) -> BackPressureConfig {
        let defaults = BackPressureConfig::default();

        let suffix_fill_threshold = self.suffix_fill_threshold.unwrap_or(defaults.suffix_fill_threshold).clamp(0.0, 1.0);

        let suffix_rate_threshold = self
            .suffix_rate_threshold
            .unwrap_or(defaults.suffix_rate_threshold)
            .clamp(0.0, suffix_fill_threshold);

        BackPressureConfig {
            check_window_ms: self.check_window_ms.unwrap_or(defaults.check_window_ms),
            max_timeout_ms: self.max_timeout_ms.unwrap_or(defaults.max_timeout_ms),
            min_timeout_ms: self.min_timeout_ms.unwrap_or(defaults.min_timeout_ms),
            suffix_max_size: self.suffix_max_size.unwrap_or(defaults.suffix_max_size),
            suffix_fill_threshold,
            suffix_rate_threshold,
            rate_delta_threshold: self.rate_delta_threshold.or(defaults.rate_delta_threshold),
        }
    }
}
