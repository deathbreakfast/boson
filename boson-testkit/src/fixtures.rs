//! Synthetic task names, JSON payloads, and task registration helpers.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

use boson_core::{BosonError, ExecutionContext};
use boson_core::{RateLimitPolicy, RetryPolicy};
use boson_runtime::{InvokeFn, TaskDefaults, TaskDescriptor, TaskRegistry};

const TESTKIT_POOL: &str = "global";
const UNLIMITED_RATE: u32 = 0;

/// Policy overrides applied when registering synthetic tasks.
#[derive(Debug, Clone, Copy)]
pub struct TaskPolicy {
    /// Default retry max attempts on descriptor.
    pub max_attempts: u32,
    /// Default retry base delay ms.
    pub base_delay_ms: u64,
    /// Max in-flight rate limit (`0` = unlimited).
    pub max_in_flight: u32,
    /// Max enqueues per second (`0` = unlimited).
    pub max_enqueue_per_second: u32,
}

impl Default for TaskPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay_ms: 0,
            max_in_flight: UNLIMITED_RATE,
            max_enqueue_per_second: UNLIMITED_RATE,
        }
    }
}

/// Default actor JSON for test enqueues.
#[must_use]
pub fn system_actor() -> serde_json::Value {
    serde_json::json!({"System": {"operation": "testkit"}})
}

/// Empty params object.
#[must_use]
pub fn empty_params() -> serde_json::Value {
    serde_json::json!({})
}

static NOOP_HITS: AtomicUsize = AtomicUsize::new(0);
static COUNTING_HITS: AtomicUsize = AtomicUsize::new(0);
static FAIL_REMAINING: AtomicU32 = AtomicU32::new(0);

/// Number of times the shared noop handler ran (test introspection).
pub fn noop_hit_count() -> usize {
    NOOP_HITS.load(Ordering::SeqCst)
}

/// Reset noop hit counter before a scenario.
pub fn reset_noop_hits() {
    NOOP_HITS.store(0, Ordering::SeqCst);
}

/// Number of times the shared counting handler ran.
pub fn counting_hit_count() -> usize {
    COUNTING_HITS.load(Ordering::SeqCst)
}

/// Reset counting handler hit counter.
pub fn reset_counting_hits() {
    COUNTING_HITS.store(0, Ordering::SeqCst);
}

/// Remaining fail invocations for [`register_fail_n_then_ok_task`].
pub fn fail_remaining() -> u32 {
    FAIL_REMAINING.load(Ordering::SeqCst)
}

/// Configure fail-until-ok handler for the next scenario.
pub fn set_fail_remaining(n: u32) {
    FAIL_REMAINING.store(n, Ordering::SeqCst);
}

/// Handler hit count for known test task names.
#[must_use]
pub fn handler_hit_count(task: &str) -> usize {
    match task {
        "noop" => noop_hit_count(),
        "counting" => counting_hit_count(),
        _ if task.starts_with("counting") => counting_hit_count(),
        _ if task.starts_with("noop") => noop_hit_count(),
        _ => 0,
    }
}

fn noop_invoke(
    _ctx: Box<dyn ExecutionContext>,
    _params: serde_json::Value,
) -> Pin<Box<dyn Future<Output = boson_core::Result<()>> + Send + 'static>> {
    Box::pin(async {
        NOOP_HITS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
}

fn counting_invoke(
    _ctx: Box<dyn ExecutionContext>,
    _params: serde_json::Value,
) -> Pin<Box<dyn Future<Output = boson_core::Result<()>> + Send + 'static>> {
    Box::pin(async {
        COUNTING_HITS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
}

fn fail_invoke(
    _ctx: Box<dyn ExecutionContext>,
    _params: serde_json::Value,
) -> Pin<Box<dyn Future<Output = boson_core::Result<()>> + Send + 'static>> {
    Box::pin(async { Err(BosonError::Internal("testkit fail task".into())) })
}

fn fail_n_then_ok_invoke(
    _ctx: Box<dyn ExecutionContext>,
    _params: serde_json::Value,
) -> Pin<Box<dyn Future<Output = boson_core::Result<()>> + Send + 'static>> {
    Box::pin(async {
        let remaining = FAIL_REMAINING.load(Ordering::SeqCst);
        if remaining > 0 {
            FAIL_REMAINING.fetch_sub(1, Ordering::SeqCst);
            Err(BosonError::Internal("testkit fail n then ok".into()))
        } else {
            Ok(())
        }
    })
}

/// Register the shared noop handler under `name` on pool `global`.
pub fn register_noop_task(registry: &mut TaskRegistry, name: &'static str) {
    register_noop_task_on_pool(registry, name, TESTKIT_POOL);
}

/// Register noop with an explicit signature hash (signature versioning tests).
pub fn register_noop_task_with_signature_hash(
    registry: &mut TaskRegistry,
    name: &'static str,
    signature_hash: u64,
) {
    register_task_with_signature_hash(
        registry,
        name,
        noop_invoke,
        TaskPolicy::default(),
        1,
        TESTKIT_POOL,
        signature_hash,
    );
}

/// Register noop on an explicit pool (partition sweep / multi-pool bench).
pub fn register_noop_task_on_pool(
    registry: &mut TaskRegistry,
    name: &'static str,
    pool: &'static str,
) {
    register_task_with_pool(registry, name, noop_invoke, TaskPolicy::default(), 1, pool);
}

/// Register noop with explicit default priority (lower = higher priority).
pub fn register_noop_task_with_priority(
    registry: &mut TaskRegistry,
    name: &'static str,
    priority: i32,
) {
    register_task_with_priority(registry, name, noop_invoke, TaskPolicy::default(), priority);
}

/// Register a handler that increments [`counting_hit_count`].
pub fn register_counting_task(registry: &mut TaskRegistry, name: &'static str) {
    register_task_with_policy(registry, name, counting_invoke, TaskPolicy::default());
}

/// Register a handler that always fails.
pub fn register_fail_task(registry: &mut TaskRegistry, name: &'static str) {
    register_task_with_policy(
        registry,
        name,
        fail_invoke,
        TaskPolicy {
            max_attempts: 1,
            ..TaskPolicy::default()
        },
    );
}

/// Register a handler that always fails with `max_attempts` retries (exhaustion → terminal Failed).
pub fn register_fail_exhaustion_task(
    registry: &mut TaskRegistry,
    name: &'static str,
    max_attempts: u32,
) {
    register_task_with_policy(
        registry,
        name,
        fail_invoke,
        TaskPolicy {
            max_attempts: max_attempts.max(1),
            base_delay_ms: 0,
            ..TaskPolicy::default()
        },
    );
}

/// Register a handler that fails `fail_count` times then succeeds.
pub fn register_fail_n_then_ok_task(
    registry: &mut TaskRegistry,
    name: &'static str,
    fail_count: u32,
) {
    set_fail_remaining(fail_count);
    register_task_with_policy(
        registry,
        name,
        fail_n_then_ok_invoke,
        TaskPolicy {
            max_attempts: fail_count.saturating_add(1).max(1),
            base_delay_ms: 0,
            ..TaskPolicy::default()
        },
    );
}

/// Register with `max_in_flight = 1`.
pub fn register_rate_limited_in_flight_task(registry: &mut TaskRegistry, name: &'static str) {
    register_task_with_policy(
        registry,
        name,
        noop_invoke,
        TaskPolicy {
            max_in_flight: 1,
            ..TaskPolicy::default()
        },
    );
}

/// Register with `max_enqueue_per_second = 1`.
pub fn register_rate_limited_eps_task(registry: &mut TaskRegistry, name: &'static str) {
    register_task_with_policy(
        registry,
        name,
        noop_invoke,
        TaskPolicy {
            max_enqueue_per_second: 1,
            ..TaskPolicy::default()
        },
    );
}

/// Register a synthetic task with explicit policy defaults on the descriptor.
pub fn register_task_with_policy(
    registry: &mut TaskRegistry,
    name: &'static str,
    invoke: InvokeFn,
    policy: TaskPolicy,
) {
    register_task_with_priority(registry, name, invoke, policy, 1);
}

/// Register a synthetic task with explicit priority on the descriptor.
pub fn register_task_with_priority(
    registry: &mut TaskRegistry,
    name: &'static str,
    invoke: InvokeFn,
    policy: TaskPolicy,
    priority: i32,
) {
    register_task_with_pool(registry, name, invoke, policy, priority, TESTKIT_POOL);
}

fn register_task_with_pool(
    registry: &mut TaskRegistry,
    name: &'static str,
    invoke: InvokeFn,
    policy: TaskPolicy,
    priority: i32,
    pool: &'static str,
) {
    register_task_with_signature_hash(registry, name, invoke, policy, priority, pool, 0);
}

fn register_task_with_signature_hash(
    registry: &mut TaskRegistry,
    name: &'static str,
    invoke: InvokeFn,
    policy: TaskPolicy,
    priority: i32,
    pool: &'static str,
    signature_hash: u64,
) {
    let defaults = TaskDefaults {
        priority,
        pool,
        retry: RetryPolicy {
            max_attempts: policy.max_attempts,
            base_delay_ms: policy.base_delay_ms,
            backoff_multiplier: 2.0,
            max_delay_ms: 30_000,
        },
        rate: RateLimitPolicy {
            max_in_flight: policy.max_in_flight,
            max_enqueue_per_second: policy.max_enqueue_per_second,
        },
    };
    let desc: &'static TaskDescriptor = Box::leak(Box::new(TaskDescriptor::with_defaults(
        name,
        invoke,
        "{}",
        signature_hash,
        defaults,
    )));
    registry.register(desc);
}

/// Assert that `name` is registered in the task registry.
///
/// Useful in integration tests that verify Quark inventory / link closure for `#[boson::task]`
/// handlers.
///
/// # Panics
///
/// Panics with the sorted registry contents when `name` is missing.
pub fn assert_task_registered(registry: &TaskRegistry, name: &str) {
    assert!(
        registry.get(name).is_some(),
        "expected task `{name}` in registry; found {:?}",
        registry.sorted_task_names()
    );
}
