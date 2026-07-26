//! Error types for Boson core operations.

use std::error::Error as StdError;

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
    #[error("backend error: {message}")]
    Backend {
        /// Operator-safe summary (stable for logs and HTTP bodies).
        message: String,
        /// Optional underlying adapter error for `Error::source` chains.
        #[source]
        source: Option<Box<dyn StdError + Send + Sync>>,
    },

    /// Internal error.
    #[error("internal error: {message}")]
    Internal {
        /// Operator-safe summary.
        message: String,
        /// Optional underlying cause for `Error::source` chains.
        #[source]
        source: Option<Box<dyn StdError + Send + Sync>>,
    },

    /// Enqueue blocked by rate limit or in-flight cap; caller should retry after backoff.
    #[error("enqueue rate limited for task: {0}")]
    RateLimited(String),

    /// Named queue backend not registered on the router.
    #[error("unknown queue backend: {0}")]
    UnknownBackend(String),
}

impl BosonError {
    /// Backend failure without an underlying source.
    #[must_use]
    pub fn backend(message: impl Into<String>) -> Self {
        Self::Backend {
            message: message.into(),
            source: None,
        }
    }

    /// Backend failure wrapping an underlying error.
    #[must_use]
    pub fn backend_source(
        message: impl Into<String>,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        Self::Backend {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    /// Internal failure without an underlying source.
    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
            source: None,
        }
    }

    /// Internal failure wrapping an underlying error.
    #[must_use]
    pub fn internal_source(
        message: impl Into<String>,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        Self::Internal {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    /// Message text for [`Self::Backend`], if this is a backend error.
    #[must_use]
    pub fn backend_message(&self) -> Option<&str> {
        match self {
            Self::Backend { message, .. } => Some(message.as_str()),
            _ => None,
        }
    }

    /// True when a backend adapter reported a unique / duplicate-key constraint failure.
    #[must_use]
    pub fn is_backend_unique_violation(&self) -> bool {
        self.backend_message().is_some_and(|msg| {
            msg.contains("UNIQUE") || msg.contains("unique") || msg.contains("Duplicate")
        })
    }
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

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;

    use super::BosonError;

    #[derive(Debug, thiserror::Error)]
    #[error("dial failed")]
    struct DialFailed;

    #[test]
    fn backend_source_is_reachable_via_std_error() {
        let err = BosonError::backend_source("nats connect", DialFailed);
        assert!(err
            .backend_message()
            .is_some_and(|m| m.contains("nats connect")));
        assert!(err
            .source()
            .is_some_and(|s| s.to_string().contains("dial failed")));
    }

    #[test]
    fn unique_violation_helper_matches_message() {
        let err = BosonError::backend("sql backend: UNIQUE constraint failed");
        assert!(err.is_backend_unique_violation());
        assert!(!BosonError::backend("timeout").is_backend_unique_violation());
    }
}
