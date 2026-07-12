//! Signature hash mismatch fails job execution via the manual worker.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use boson_backend_mem::MemQueueBackend;
use boson_core::{
    ExecutionContext, ExecutionContextFactory, IdentityError, JobStatus, QueueBackend,
};
use boson_runtime::{Boson, TaskDescriptor, TaskDefaults, TaskRegistry};

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

fn noop_invoke(
    _ctx: Box<dyn ExecutionContext>,
    _params: serde_json::Value,
) -> Pin<Box<dyn Future<Output = boson_core::Result<()>> + Send + 'static>> {
    Box::pin(async { Ok(()) })
}

fn register_sig_task(registry: &mut TaskRegistry, hash: u64) {
    let desc: &'static TaskDescriptor = Box::leak(Box::new(TaskDescriptor::with_defaults(
        "sig_task",
        noop_invoke,
        "{}",
        hash,
        TaskDefaults::standard(),
    )));
    registry.register(desc);
}

#[tokio::test(flavor = "multi_thread")]
async fn manual_worker_marks_job_failed_on_signature_mismatch() {
    let backend: Arc<dyn QueueBackend> = Arc::new(MemQueueBackend::new());

    let mut registry_v1 = TaskRegistry::new();
    register_sig_task(&mut registry_v1, 1);
    let boson = Boson::builder()
        .queue_backend(Arc::clone(&backend))
        .execution_context_factory(TestFactory)
        .registry(Arc::new(registry_v1))
        .without_worker()
        .build()
        .expect("build");

    boson
        .enqueue("sig_task", serde_json::json!({"System": {}}), serde_json::json!({}), None)
        .await
        .expect("enqueue");

    let mut registry_v2 = TaskRegistry::new();
    register_sig_task(&mut registry_v2, 2);
    let (_boson2, manual) = Boson::builder()
        .queue_backend(backend)
        .execution_context_factory(TestFactory)
        .registry(Arc::new(registry_v2))
        .without_worker()
        .build_manual()
        .expect("build_manual");

    for _ in 0..8 {
        manual.try_run_next().await;
    }

    let failed = boson
        .list_jobs(Some(JobStatus::Failed), 0, 10)
        .await
        .expect("list");
    assert_eq!(failed.len(), 1);
}
