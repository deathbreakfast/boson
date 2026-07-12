//! Integration tests for `#[boson::task]` inventory registration and dispatch.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use boson_backend_mem::MemQueueBackend;
use boson_core::{ExecutionContext, JobStatus, JsonExecutionContextFactory, QueueBackend};
use boson_macros::task;
use boson_runtime::{configure, Boson, ManualWorker};

static INVENTORY_RUNS: AtomicUsize = AtomicUsize::new(0);

#[task(name = "inventory_echo")]
#[allow(clippy::unused_async)] // task handlers must be async for the macro contract
async fn inventory_echo(
    _ctx: Box<dyn ExecutionContext>,
    message: String,
) -> boson_core::Result<()> {
    INVENTORY_RUNS.fetch_add(1, Ordering::SeqCst);
    assert_eq!(message, "hello");
    Ok(())
}

#[test]
fn macro_task_in_task_registry() {
    let registry = boson_runtime::TaskRegistry::auto_discover();
    let names = registry.sorted_task_names();
    assert!(
        names.contains(&"inventory_echo"),
        "expected inventory_echo in {names:?}"
    );
}

#[tokio::test]
async fn macro_task_enqueue_and_dispatch() {
    INVENTORY_RUNS.store(0, Ordering::SeqCst);

    let (boson, manual) = Boson::builder()
        .queue_backend(Arc::new(MemQueueBackend::new()) as Arc<dyn QueueBackend>)
        .execution_context_factory(JsonExecutionContextFactory)
        .auto_registry()
        .without_worker()
        .build_manual()
        .expect("build");

    configure(boson.clone());

    let job_id = InventoryEcho::send_with(
        serde_json::json!({"System": {"operation": "test"}}),
        InventoryEchoParams {
            message: "hello".into(),
        },
    )
    .await
    .expect("enqueue");

    assert!(manual.try_run_next().await);

    let job = boson.get_job(&job_id).await.unwrap().expect("job");
    assert_eq!(job.status, JobStatus::Success);
    assert_eq!(INVENTORY_RUNS.load(Ordering::SeqCst), 1);
}

// Silence unused import warning when only registry test runs in isolation.
#[allow(dead_code)]
fn _manual_worker_type(_: ManualWorker) {}
