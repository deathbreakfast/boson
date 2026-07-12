//! Integration tests for boson-runtime on mem backend.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use boson_backend_mem::MemQueueBackend;
use boson_core::{
    ExecutionContext, ExecutionContextFactory, IdentityError, JobStatus, QueueBackend,
};
use boson_runtime::{Boson, TaskDescriptor, TaskRegistry};

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
