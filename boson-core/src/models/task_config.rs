//! Task config model — priority, pool, retry, rate limits, and idempotency.
//!
//! [`TaskConfig`] is persisted per task. On enqueue, Boson merges descriptor defaults with
//! any stored config to set [`Job`](crate::Job) priority, pool, and policies.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// How backends enforce enqueue idempotency when a job key is present.
///
/// On Scylla, [`IdempotencyMode::Lwt`] uses a lightweight transaction; [`IdempotencyMode::None`]
/// skips that path (at-least-once). Mem/SQL honor the same policy for check-then-reuse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdempotencyMode {
    /// Exactly-once under concurrent enqueue (default).
    #[default]
    Lwt,
    /// At-least-once; skip idempotency coordination (higher throughput).
    None,
}

impl IdempotencyMode {
    /// Parse from a config string (`lwt` / `none`).
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "lwt" => Some(Self::Lwt),
            "none" => Some(Self::None),
            _ => None,
        }
    }
}

/// Retry policy for a task.
///
/// On handler failure, the worker reschedules while `job.attempt < max_attempts`.
/// Delay before the next attempt (milliseconds):
///
/// `min(base_delay_ms × backoff_multiplier^(attempt - 1), max_delay_ms)`
///
/// where `attempt` is the 1-based attempt number on the job.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Maximum retry attempts after the first failure (`0` = no retries).
    pub max_attempts: u32,
    /// Base delay in ms before the first retry.
    pub base_delay_ms: u64,
    /// Exponential backoff multiplier applied per attempt.
    pub backoff_multiplier: f64,
    /// Upper cap on retry delay in ms.
    pub max_delay_ms: u64,
}

/// Rate limiting for enqueue backpressure.
///
/// `max_in_flight == 0` and `max_enqueue_per_second == 0` mean unlimited (default).
///
/// When a limit is exceeded, [`Boson::enqueue`](https://docs.rs/boson-runtime/latest/boson_runtime/struct.Boson.html#method.enqueue) returns
/// [`BosonError::RateLimited`](crate::BosonError::RateLimited); callers should retry with backoff.
///
/// # Example
///
/// ```rust
/// use boson_core::RateLimitPolicy;
///
/// // At most 10 active jobs and 5 enqueues per second for one task.
/// let policy = RateLimitPolicy {
///     max_in_flight: 10,
///     max_enqueue_per_second: 5,
/// };
/// ```
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct RateLimitPolicy {
    /// Max jobs for this task in `queued` + `running` at once. `0` = no limit.
    pub max_in_flight: u32,
    /// Max successful enqueues per wall-clock second per process. `0` = no limit.
    pub max_enqueue_per_second: u32,
}

/// Per-task config persisted for admin UI and enqueue defaults.
///
/// Seeded from [`task`](https://docs.rs/boson-macros) macro attributes on first enqueue; admins can
/// later upsert via [`QueueBackend::upsert_task_config`](crate::QueueBackend::upsert_task_config)
/// or the axum `/tasks/{name}/config` route. On enqueue, Boson merges descriptor defaults with any
/// stored config to set [`Job`](crate::Job) priority, pool, and policies.
///
/// Getting started: [define tasks](https://docs.rs/uf-boson/latest/boson/index.html#3-define-tasks).
///
/// # Example — macro attrs become the initial config
///
/// ```ignore
/// use boson::task;
///
/// #[task(
///     name = "notify",
///     priority = 10,
///     pool = "alerts",
///     max_attempts = 5,
///     max_in_flight = 20
/// )]
/// async fn notify(ctx: Box<dyn boson::ExecutionContext>, message: String) -> boson_core::Result<()> {
///     let _ = (ctx, message);
///     Ok(())
/// }
/// // First Notify::send_with(...) persists a TaskConfig shaped like those attributes.
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskConfig {
    /// Task name (unique key).
    pub task_name: String,
    /// Override priority (lower = higher priority).
    pub priority: i32,
    /// Override pool name.
    pub pool: String,
    /// Retry policy override.
    pub retry_policy: RetryPolicy,
    /// Enqueue rate limits (optional).
    pub rate_limit_policy: RateLimitPolicy,
    /// Per-task idempotency override (`None` = inherit runtime / builder default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_mode: Option<IdempotencyMode>,
    /// When created/updated.
    pub updated_at: DateTime<Utc>,
}

impl TaskConfig {
    /// Create default config for a task name.
    ///
    /// # Examples
    ///
    /// ```
    /// use boson_core::TaskConfig;
    ///
    /// let config = TaskConfig::default_for("notify");
    /// assert_eq!(config.task_name, "notify");
    /// assert_eq!(config.pool, "global");
    /// assert_eq!(config.priority, 1);
    /// ```
    #[must_use]
    pub fn default_for(task_name: &str) -> Self {
        Self {
            task_name: task_name.to_string(),
            priority: 1,
            pool: "global".to_string(),
            retry_policy: RetryPolicy::default(),
            rate_limit_policy: RateLimitPolicy::default(),
            idempotency_mode: None,
            updated_at: Utc::now(),
        }
    }

    /// Resolve effective idempotency mode (task override, else `runtime_default`).
    #[must_use]
    pub fn resolved_idempotency_mode(&self, runtime_default: IdempotencyMode) -> IdempotencyMode {
        self.idempotency_mode.unwrap_or(runtime_default)
    }

    /// Build config from explicit policy defaults (typically from a task descriptor).
    #[must_use]
    pub fn from_policy_defaults(
        task_name: &str,
        priority: i32,
        pool: impl Into<String>,
        retry_policy: RetryPolicy,
        rate_limit_policy: RateLimitPolicy,
        idempotency_mode: Option<IdempotencyMode>,
    ) -> Self {
        Self {
            task_name: task_name.to_string(),
            priority,
            pool: pool.into(),
            retry_policy,
            rate_limit_policy,
            idempotency_mode,
            updated_at: Utc::now(),
        }
    }

    /// Fill [`idempotency_mode`](Self::idempotency_mode) from the runtime default when unset.
    #[must_use]
    pub const fn with_runtime_idempotency_fallback(
        mut self,
        runtime_default: IdempotencyMode,
    ) -> Self {
        if self.idempotency_mode.is_none() {
            self.idempotency_mode = Some(runtime_default);
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_policy_defaults_sets_fields() {
        let config = TaskConfig::from_policy_defaults(
            "notify",
            2,
            "urgent",
            RetryPolicy {
                max_attempts: 5,
                base_delay_ms: 100,
                backoff_multiplier: 2.0,
                max_delay_ms: 1000,
            },
            RateLimitPolicy {
                max_in_flight: 10,
                max_enqueue_per_second: 3,
            },
            Some(IdempotencyMode::None),
        );
        assert_eq!(config.task_name, "notify");
        assert_eq!(config.priority, 2);
        assert_eq!(config.pool, "urgent");
        assert_eq!(config.retry_policy.max_attempts, 5);
        assert_eq!(config.rate_limit_policy.max_in_flight, 10);
        assert_eq!(config.idempotency_mode, Some(IdempotencyMode::None));
    }

    #[test]
    fn runtime_idempotency_fallback_fills_none_only() {
        let filled = TaskConfig::default_for("t").with_runtime_idempotency_fallback(IdempotencyMode::Lwt);
        assert_eq!(filled.idempotency_mode, Some(IdempotencyMode::Lwt));

        let kept = TaskConfig::default_for("t");
        let mut kept = kept;
        kept.idempotency_mode = Some(IdempotencyMode::None);
        let kept = kept.with_runtime_idempotency_fallback(IdempotencyMode::Lwt);
        assert_eq!(kept.idempotency_mode, Some(IdempotencyMode::None));
    }
}
