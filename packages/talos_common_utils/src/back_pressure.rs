#[derive(Debug, Default, Clone)]
pub struct TalosBackPressureConfig {
    /// Current count which is used to check against max and min to determine if back-pressure should be applied.
    pub current: u32,
    /// Flag denotes if back pressure is enabled, and therefore the thread cannot receive more messages (candidate/decisions) to process.
    pub is_enabled: bool,
    /// Max count before back pressue is enabled.
    /// if `None`, back pressure logic will not apply.
    pub max_threshold: Option<u32>,
    /// `min_threshold` helps to prevent immediate toggle between switch on and off of the backpressure.
    /// Batch of items to process, when back pressure is enabled before disable logic is checked?
    /// if None, no minimum check is done, and as soon as the count is below the max_threshold, back pressure is disabled.
    pub min_threshold: Option<u32>,
}

impl TalosBackPressureConfig {
    pub fn new(min_threshold: Option<u32>, max_threshold: Option<u32>) -> Self {
        assert!(
            min_threshold.le(&max_threshold),
            "min_threshold ({min_threshold:?}) must be less or equal to the max_threshold ({max_threshold:?})"
        );
        Self {
            max_threshold,
            min_threshold,
            is_enabled: false,
            current: 0,
        }
    }

    pub fn increment_current(&mut self) {
        self.current += 1;
    }

    pub fn decrement_current(&mut self) {
        self.current -= 1;
    }

    /// Get the remaining available count before hitting the max_threshold, and thereby enabling back pressure.
    pub fn get_remaining_count(&self) -> Option<u32> {
        self.max_threshold.map(|max| max.saturating_sub(self.current))
    }

    pub fn update_back_pressure_flag(&mut self, is_enabled: bool) {
        self.is_enabled = is_enabled
    }

    /// Looks at the `max_threshold` and `min_threshold` to determine if back pressure should be enabled. `max_threshold` is used to determine when to enable the back-pressure
    /// whereas, `min_threshold` is used to look at the lower bound
    /// - `max_threshold` - Use to determine the upper bound of maximum items allowed.
    ///                     If this is set to `None`, no back pressure will be applied.
    ///                     `max_threshold` is
    /// - `min_threshold` - Use to determine the lower bound of maximum items allowed. If this is set to `None`, no back pressure will be applied.
    pub fn should_apply_back_pressure(&mut self) -> bool {
        let current_count = self.current;
        match self.max_threshold {
            Some(max_threshold) => {
                // if not enabled, only check against the max_threshold.
                if current_count >= max_threshold {
                    true
                } else {
                    // if already enabled, check when is it safe to remove.

                    // If there is Some(`min_threshold`), then return true if current_count > `min_threshold`.
                    // If there is Some(`min_threshold`), then return false if current_count <= `min_threshold`.
                    // If None, then return false.
                    match self.min_threshold {
                        Some(min_threshold) if self.is_enabled => current_count > min_threshold,
                        _ => false,
                    }
                }
            }
            // if None, then we don't apply any back pressure.
            None => false,
        }
    }
}
