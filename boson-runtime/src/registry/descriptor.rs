//! Task descriptor for auto-registration and manual registration.
//!
//! A [`TaskDescriptor`] is static metadata for one handler. Defaults seed the initial
//! [`TaskConfig`](boson_core::TaskConfig) on first enqueue; admin updates can override
//! priority, pool, retry, and rate limits at runtime.

use std::future::Future;
use std::pin::Pin;

use boson_core::{ExecutionContext, IdempotencyMode, RateLimitPolicy, Result, RetryPolicy};
use serde_json::Value;

/// Invokes a registered task with execution context and JSON parameters.
pub type InvokeFn = fn(
    Box<dyn ExecutionContext>,
    Value,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;

/// Default retry, rate, priority, and pool settings for a registered task.
#[derive(Debug, Clone, Copy)]
pub struct TaskDefaults {
    /// Default priority (lower = higher priority).
    pub priority: i32,
    /// Default pool name.
    pub pool: &'static str,
    /// Default retry policy.
    pub retry: RetryPolicy,
    /// Default enqueue rate limits.
    pub rate: RateLimitPolicy,
}

impl TaskDefaults {
    /// Standard production-like defaults for manual registration.
    ///
    /// - priority `1`, pool `"global"`
    /// - retry: 3 attempts, 1000 ms base delay, `2.0×` multiplier, `30_000` ms cap
    /// - rate: 100 max in-flight, 50 enqueues per second
    #[must_use]
    pub const fn standard() -> Self {
        Self {
            priority: 1,
            pool: "global",
            retry: RetryPolicy {
                max_attempts: 3,
                base_delay_ms: 1000,
                backoff_multiplier: 2.0,
                max_delay_ms: 30_000,
            },
            rate: RateLimitPolicy {
                max_in_flight: 100,
                max_enqueue_per_second: 50,
            },
        }
    }
}

/// Descriptor for a registered task.
///
/// Registration defaults flow into [`TaskConfig`](boson_core::TaskConfig) on first enqueue.
/// Use [`TaskDescriptor::with_defaults`] to set retry, rate, priority, and pool in one call.
///
/// ## Signature versioning
///
/// - [`signature_json`](Self::signature_json) — JSON schema string describing task parameters
///   (convention for tooling; Boson does not validate against it at runtime).
/// - [`signature_hash`](Self::signature_hash) — hash of the parameter schema/version. Stored on
///   each [`Job`](boson_core::Job) at enqueue; if the registered task's hash changes while a job
///   is still active, dispatch returns [`BosonError::SignatureMismatch`](boson_core::BosonError::SignatureMismatch).
///   Bump `signature_hash` when you change parameter shape incompatibly.
#[derive(Clone, Copy)]
pub struct TaskDescriptor {
    /// Unique task name (registry key and enqueue target).
    pub name: &'static str,
    /// Function to invoke the task.
    pub invoke: InvokeFn,
    /// JSON schema string for parameters (documentation / tooling; not validated at runtime).
    pub signature_json: &'static str,
    /// Version hash checked against enqueued jobs; change when parameters change incompatibly.
    pub signature_hash: u64,
    /// Default priority (lower = higher priority).
    pub default_priority: i32,
    /// Default pool name for worker assignment.
    pub default_pool: &'static str,
    /// Default retry max attempts (see [`RetryPolicy::max_attempts`]).
    pub default_retry_max_attempts: u32,
    /// Default retry base delay ms (see [`RetryPolicy::base_delay_ms`]).
    pub default_retry_base_delay_ms: u64,
    /// Default retry backoff multiplier (see [`RetryPolicy::backoff_multiplier`]).
    pub default_retry_backoff_multiplier: f64,
    /// Default retry max delay ms (see [`RetryPolicy::max_delay_ms`]).
    pub default_retry_max_delay_ms: u64,
    /// Default max in-flight jobs (see [`RateLimitPolicy::max_in_flight`]; `0` = unlimited).
    pub default_rate_max_in_flight: u32,
    /// Default max enqueues per second (see [`RateLimitPolicy::max_enqueue_per_second`]; `0` = unlimited).
    pub default_rate_max_enqueue_per_second: u32,
    /// Per-task idempotency override (`None` = inherit runtime default).
    pub default_idempotency_mode: Option<IdempotencyMode>,
}

impl TaskDescriptor {
    /// Minimal descriptor for tests (`signature_json` `"{}"`, `signature_hash` `0`).
    pub const fn new(name: &'static str, invoke: InvokeFn) -> Self {
        Self::with_defaults(name, invoke, "{}", 0, TaskDefaults::standard())
    }

    /// Descriptor with grouped policy defaults.
    pub const fn with_defaults(
        name: &'static str,
        invoke: InvokeFn,
        signature_json: &'static str,
        signature_hash: u64,
        defaults: TaskDefaults,
    ) -> Self {
        Self {
            name,
            invoke,
            signature_json,
            signature_hash,
            default_priority: defaults.priority,
            default_pool: defaults.pool,
            default_retry_max_attempts: defaults.retry.max_attempts,
            default_retry_base_delay_ms: defaults.retry.base_delay_ms,
            default_retry_backoff_multiplier: defaults.retry.backoff_multiplier,
            default_retry_max_delay_ms: defaults.retry.max_delay_ms,
            default_rate_max_in_flight: defaults.rate.max_in_flight,
            default_rate_max_enqueue_per_second: defaults.rate.max_enqueue_per_second,
            default_idempotency_mode: None,
        }
    }

    /// Descriptor with explicit per-field policy defaults.
    ///
    /// Used by the [`#[task]`](https://docs.rs/boson-macros/latest/boson_macros/attr.task.html) attribute when policy fields are set on the handler.
    /// Prefer [`Self::with_defaults`] for manual registration in tests.
    #[allow(clippy::too_many_arguments)]
    pub const fn with_policy(
        name: &'static str,
        invoke: InvokeFn,
        signature_json: &'static str,
        signature_hash: u64,
        priority: i32,
        pool: &'static str,
        max_attempts: u32,
        base_delay_ms: u64,
        backoff_multiplier: f64,
        max_delay_ms: u64,
        max_in_flight: u32,
        max_enqueue_per_second: u32,
        idempotency_mode: Option<IdempotencyMode>,
    ) -> Self {
        Self {
            name,
            invoke,
            signature_json,
            signature_hash,
            default_priority: priority,
            default_pool: pool,
            default_retry_max_attempts: max_attempts,
            default_retry_base_delay_ms: base_delay_ms,
            default_retry_backoff_multiplier: backoff_multiplier,
            default_retry_max_delay_ms: max_delay_ms,
            default_rate_max_in_flight: max_in_flight,
            default_rate_max_enqueue_per_second: max_enqueue_per_second,
            default_idempotency_mode: idempotency_mode,
        }
    }

    /// Materialize descriptor defaults into a [`TaskConfig`](boson_core::TaskConfig).
    #[must_use]
    pub fn to_task_config(&self) -> boson_core::TaskConfig {
        boson_core::TaskConfig::from_policy_defaults(
            self.name,
            self.default_priority,
            self.default_pool,
            RetryPolicy {
                max_attempts: self.default_retry_max_attempts,
                base_delay_ms: self.default_retry_base_delay_ms,
                backoff_multiplier: self.default_retry_backoff_multiplier,
                max_delay_ms: self.default_retry_max_delay_ms,
            },
            RateLimitPolicy {
                max_in_flight: self.default_rate_max_in_flight,
                max_enqueue_per_second: self.default_rate_max_enqueue_per_second,
            },
            self.default_idempotency_mode,
        )
    }
}

impl std::fmt::Debug for TaskDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskDescriptor")
            .field("name", &self.name)
            .field("signature_json", &self.signature_json)
            .field("signature_hash", &self.signature_hash)
            .field("default_priority", &self.default_priority)
            .field("default_pool", &self.default_pool)
            .field("default_retry_max_attempts", &self.default_retry_max_attempts)
            .field("default_retry_base_delay_ms", &self.default_retry_base_delay_ms)
            .field(
                "default_retry_backoff_multiplier",
                &self.default_retry_backoff_multiplier,
            )
            .field("default_retry_max_delay_ms", &self.default_retry_max_delay_ms)
            .field("default_rate_max_in_flight", &self.default_rate_max_in_flight)
            .field(
                "default_rate_max_enqueue_per_second",
                &self.default_rate_max_enqueue_per_second,
            )
            .field("default_idempotency_mode", &self.default_idempotency_mode)
            .field("invoke", &"<fn>")
            .finish()
    }
}

quark::inventory::collect!(TaskDescriptor);

impl quark::Registrable for TaskDescriptor {
    fn registry_key(&self) -> &str {
        self.name
    }
}
