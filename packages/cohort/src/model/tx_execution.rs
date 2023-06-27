use crate::metrics::{Stats, TxExecSpans};

pub enum TxExecutionOutcome {
    GaveUp { stats: Stats },
    Executed { stats: Stats },
}

pub enum TxExecutionResult {
    RetriableError { reason: TxNonFatalError, metrics: TxExecSpans },
    Success { metrics: TxExecSpans },
}

pub enum TxNonFatalError {
    ErrorAborted,
    ErrorIsolation,
    ErrorValidation,
}
pub struct BankResult {
    pub is_aborted: bool,
    pub is_tx_isolation_error: bool,
    pub metrics: TxExecSpans,
}
