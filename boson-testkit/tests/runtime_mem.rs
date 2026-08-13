//! Integration tests for boson-runtime on mem backend.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::print_stderr
)] // Integration-test helpers are not covered by clippy.toml allow-*-in-tests.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use boson_backend_mem::MemQueueBackend;
use boson_core::{
    EnqueueTrust, ExecutionContext, ExecutionContextFactory, IdentityError, JobStatus,
    QueueBackend, RateLimitPolicy, RetryPolicy,
};
use boson_runtime::{Boson, TaskDefaults, TaskDescriptor, TaskRegistry};

static MANUAL_RUNS: AtomicUsize = AtomicUsize::new(0);
static SPAWN_RUNS: AtomicUsize = AtomicUsize::new(0);

struct TestCtx {
    actor_json: serde_json::Value,
}

impl ExecutionContext for TestCtx {
    fn label(&self) -> &'static str {
        "test"
    }

    fn actor_json(&self) -> &serde_json::Value {
        &self.actor_json
    }
}

struct TestFactory;
impl ExecutionContextFactory for TestFactory {
    fn build(
        &self,
        actor_json: &serde_json::Value,
    ) -> Result<Box<dyn ExecutionContext>, IdentityError> {
        Ok(Box::new(TestCtx {
            actor_json: actor_json.clone(),
        }))
    }
}

fn echo_task_manual(
    _ctx: Box<dyn ExecutionContext>,
    _params: serde_json::Value,
) -> Pin<Box<dyn Future<Output = boson_core::Result<()>> + Send + 'static>> {
    Box::pin(async {
        MANUAL_RUNS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
}

fn echo_task_spawn(
    _ctx: Box<dyn ExecutionContext>,
    _params: serde_json::Value,
) -> Pin<Box<dyn Future<Output = boson_core::Result<()>> + Send + 'static>> {
    Box::pin(async {
        SPAWN_RUNS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
}

fn register_task(registry: &mut TaskRegistry, name: &'static str, invoke: boson_runtime::InvokeFn) {
    let desc: &'static TaskDescriptor = Box::leak(Box::new(TaskDescriptor::new(name, invoke)));
    registry.register(desc);
}

#[tokio::test]
async fn enqueue_and_manual_worker() {
    MANUAL_RUNS.store(0, Ordering::SeqCst);
    let backend: Arc<dyn QueueBackend> = Arc::new(MemQueueBackend::new());
    let mut registry = TaskRegistry::new();
    register_task(&mut registry, "echo", echo_task_manual);
    let registry = Arc::new(registry);

    let (boson, manual) = Boson::builder()
        .queue_backend(Arc::clone(&backend))
        .execution_context_factory(TestFactory)
        .registry(registry)
        .without_worker()
        .build_manual()
        .expect("build");

    let job_id = boson
        .enqueue(
            "echo",
            serde_json::json!({"System": {"operation": "test"}}),
            serde_json::json!({}),
            None,
        )
        .await
        .expect("enqueue");

    assert!(manual.try_run_next().await);
    let job = boson.get_job(&job_id).await.unwrap().expect("job");
    assert_eq!(job.status, JobStatus::Success);
    assert_eq!(MANUAL_RUNS.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn spawn_worker_completes_job() {
    SPAWN_RUNS.store(0, Ordering::SeqCst);
    let backend: Arc<dyn QueueBackend> = Arc::new(MemQueueBackend::new());
    let mut registry = TaskRegistry::new();
    register_task(&mut registry, "echo", echo_task_spawn);

    let boson = Boson::builder()
        .queue_backend(backend)
        .execution_context_factory(TestFactory)
        .registry(Arc::new(registry))
        .build()
        .expect("build");

    let job_id = boson
        .enqueue(
            "echo",
            serde_json::json!({"System": {"operation": "test"}}),
            serde_json::json!({}),
            None,
        )
        .await
        .expect("enqueue");

    for _ in 0..50 {
        if let Some(job) = boson.get_job(&job_id).await.unwrap() {
            if job.status == JobStatus::Success {
                assert_eq!(SPAWN_RUNS.load(Ordering::SeqCst), 1);
                return;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("job did not complete in time");
}

#[tokio::test]
async fn external_system_actor_rejected_no_job_row() {
    let backend: Arc<dyn QueueBackend> = Arc::new(MemQueueBackend::new());
    let mut registry = TaskRegistry::new();
    register_task(&mut registry, "echo", echo_task_manual);
    let (boson, _manual) = Boson::builder()
        .queue_backend(Arc::clone(&backend))
        .execution_context_factory(TestFactory)
        .registry(Arc::new(registry))
        .without_worker()
        .build_manual()
        .expect("build");

    let err = boson
        .enqueue_with_trust(
            "echo",
            serde_json::json!({"System": {"operation": "spoof"}}),
            serde_json::json!({}),
            None,
            EnqueueTrust::External,
        )
        .await
        .expect_err("must reject System on External");
    assert!(
        err.to_string().contains("System"),
        "unexpected error: {err}"
    );
    let listed = boson.list_jobs(None, 0, 100).await.expect("list_jobs");
    assert!(
        listed.is_empty(),
        "rejected enqueue must not insert a job row"
    );
}

fn leak_secret_fail_task(
    _ctx: Box<dyn ExecutionContext>,
    _params: serde_json::Value,
) -> Pin<Box<dyn Future<Output = boson_core::Result<()>> + Send + 'static>> {
    Box::pin(async {
        Err(boson_core::BosonError::internal(
            "db failed password=hunter2 more",
        ))
    })
}

#[tokio::test]
async fn handler_error_sanitized_in_run_row() {
    let backend: Arc<dyn QueueBackend> = Arc::new(MemQueueBackend::new());
    let mut registry = TaskRegistry::new();
    let defaults = TaskDefaults {
        priority: 1,
        pool: "global",
        retry: RetryPolicy {
            max_attempts: 1,
            base_delay_ms: 0,
            backoff_multiplier: 2.0,
            max_delay_ms: 1000,
        },
        rate: RateLimitPolicy {
            max_in_flight: 0,
            max_enqueue_per_second: 0,
        },
    };
    let desc: &'static TaskDescriptor = Box::leak(Box::new(TaskDescriptor::with_defaults(
        "leak",
        leak_secret_fail_task,
        "{}",
        0,
        defaults,
    )));
    registry.register(desc);
    let (boson, manual) = Boson::builder()
        .queue_backend(backend)
        .execution_context_factory(TestFactory)
        .registry(Arc::new(registry))
        .without_worker()
        .build_manual()
        .expect("build");

    let job_id = boson
        .enqueue(
            "leak",
            serde_json::json!({"Service": {"name": "test"}}),
            serde_json::json!({}),
            None,
        )
        .await
        .expect("enqueue");
    assert!(manual.try_run_next().await);

    let runs = boson.list_runs(Some(&job_id), 0, 8).await.expect("runs");
    let run = runs.last().expect("run row");
    let msg = run.error_message.as_deref().unwrap_or("");
    assert!(
        !msg.contains("hunter2"),
        "secret must not be stored raw: {msg}"
    );
    assert!(
        msg.contains("[redacted]") || msg.contains("password"),
        "expected sanitized message, got: {msg}"
    );
}

fn panic_task(
    _ctx: Box<dyn ExecutionContext>,
    _params: serde_json::Value,
) -> Pin<Box<dyn Future<Output = boson_core::Result<()>> + Send + 'static>> {
    Box::pin(async {
        panic!("runtime_mem panic fixture");
    })
}

#[tokio::test]
async fn handler_panic_marks_job_failed() {
    let backend: Arc<dyn QueueBackend> = Arc::new(MemQueueBackend::new());
    let mut registry = TaskRegistry::new();
    let defaults = TaskDefaults {
        priority: 1,
        pool: "global",
        retry: RetryPolicy {
            max_attempts: 1,
            base_delay_ms: 0,
            backoff_multiplier: 2.0,
            max_delay_ms: 1000,
        },
        rate: RateLimitPolicy {
            max_in_flight: 0,
            max_enqueue_per_second: 0,
        },
    };
    let desc: &'static TaskDescriptor = Box::leak(Box::new(TaskDescriptor::with_defaults(
        "panic_task",
        panic_task,
        "{}",
        0,
        defaults,
    )));
    registry.register(desc);
    let (boson, manual) = Boson::builder()
        .queue_backend(backend)
        .execution_context_factory(TestFactory)
        .registry(Arc::new(registry))
        .without_worker()
        .build_manual()
        .expect("build");

    let job_id = boson
        .enqueue(
            "panic_task",
            serde_json::json!({"Service": {"name": "test"}}),
            serde_json::json!({}),
            None,
        )
        .await
        .expect("enqueue");
    assert!(manual.try_run_next().await);

    let job = boson.get_job(&job_id).await.expect("get").expect("job");
    assert_eq!(
        job.status,
        JobStatus::Failed,
        "panic must not leave job Running"
    );
    let runs = boson.list_runs(Some(&job_id), 0, 8).await.expect("runs");
    let run = runs.last().expect("run row");
    assert_eq!(run.status, boson_core::RunStatus::Failed);
    let msg = run.error_message.as_deref().unwrap_or("");
    assert!(
        msg.contains("panicked"),
        "expected handler panicked message, got: {msg}"
    );
}
