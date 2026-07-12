//! Error types for Boson core operations.

use thiserror::Error;

/// Result type alias for Boson core operations.
pub type Result<T> = std::result::Result<T, BosonError>;

/// Errors that can occur in Boson operations.
#[derive(Debug, Error)]
pub enum BosonError {
    /// Task not found in registry.
    #[error("task not found: {0}")]
    TaskNotFound(String),

    /// Job not found.
    #[error("job not found: {0}")]
    JobNotFound(String),

    /// Run not found.
    #[error("run not found: {0}")]
    RunNotFound(String),

    /// Task config not found.
    #[error("task config not found: {0}")]
    TaskConfigNotFound(String),

    /// Parameter serialization/deserialization error.
    #[error("parameter error: {0}")]
    ParamError(String),

    /// Signature mismatch between job and current task.
    #[error("signature mismatch: job expects {expected}, task has {actual}")]
    SignatureMismatch {
        /// Expected signature from the enqueued job.
        expected: String,
        /// Current task signature in the registry.
        actual: String,
    },

    /// Invalid priority or pool.
    #[error("invalid config: {0}")]
    InvalidConfig(String),

    /// Persistence / adapter backend failure.
    #[error("backend error: {0}")]
    Backend(String),

    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),

    /// Enqueue blocked by rate limit or in-flight cap; caller should retry after backoff.
    #[error("enqueue rate limited for task: {0}")]
    RateLimited(String),

    /// Named queue backend not registered on the router.
    #[error("unknown queue backend: {0}")]
    UnknownBackend(String),
}

/// Identity reconstruction failure at handler boundary.
#[derive(Debug, Error)]
pub enum IdentityError {
    /// Actor JSON could not be parsed or mapped.
    #[error("invalid actor: {0}")]
    InvalidActor(String),
}

impl From<serde_json::Error> for BosonError {
    fn from(err: serde_json::Error) -> Self {
        Self::ParamError(err.to_string())
    }
}
